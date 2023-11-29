import time

__author__ = 'alexisgallepe'

import select
from threading import Thread
from pubsub import pub
import rarest_piece
import logging
import message
import peer
import errno
import socket
import random


class PeersManager(Thread):
    def __init__(self, torrent, pieces_manager):
        Thread.__init__(self)
        # 存储已连接的对等方
        self.peers = []
        self.torrent = torrent
        # 片段管理器
        self.pieces_manager = pieces_manager
        # 管理稀缺片段，此版本未实装
        self.rarest_pieces = rarest_piece.RarestPieces(pieces_manager)
        # 记录拥有该片段的对等方数量和对等方列表
        self.pieces_by_peer = [[0, []] for _ in range(pieces_manager.number_of_pieces)]
        # 控制线程是否应该运行
        self.is_active = True

        # Events
        # 订阅两个事件，当其他模块有函数发送了这两个事件，PeersManager将相应调用self.peer_requests_piece和self.peers_bitfield来处理
        # 处理对等方请求片段的事件
        pub.subscribe(self.peer_requests_piece, 'PeersManager.PeerRequestsPiece')
        # 处理对等方bitfield更新事件，尚未实装，且从注释来看此事件会被移入RarestPieces类
        pub.subscribe(self.peers_bitfield, 'PeersManager.updatePeersBitfield')

    # 处理对等方请求片段的事件
    def peer_requests_piece(self, request=None, peer=None):
        if not request or not peer:
            logging.error("empty request/peer message")
        # 从请求信息中提取对等方所需要的片段的片段号，块偏移量和块长度
        piece_index, block_offset, block_length = request.piece_index, request.block_offset, request.block_length
        # 用提取得到的信息获取块的真实数据
        block = self.pieces_manager.get_block(piece_index, block_offset, block_length)
        # 如果该块存在，就发送给对等方
        if block:
            piece = message.Piece(piece_index, block_offset, block_length, block).to_bytes()
            peer.send_to_peer(piece)
            logging.info("Sent piece index {} to peer : {}".format(request.piece_index, peer.ip))

    # 处理对等方bitfield更新事件
    def peers_bitfield(self, bitfield=None):
        # 遍历所有片段
        for i in range(len(self.pieces_by_peer)):
            # 如果bitfield显示对等方拥有该片段，且我方尚未将该对等方记录下来
            # BUG: 此处应该删去and self.pieces_by_peer[i][0]，因为永远无法达成
            if bitfield[i] == 1 and peer not in self.pieces_by_peer[i][1] and self.pieces_by_peer[i][0]:
                self.pieces_by_peer[i][1].append(peer)
                self.pieces_by_peer[i][0] = len(self.pieces_by_peer[i][1])

    # 随机选择一个有指定数据片段且符合条件的对等方
    def get_random_peer_having_piece(self, index):
        # 候选列表
        ready_peers = []

        for peer in self.peers:
            # 如果对等方没有被连续请求，没有将本客户端阻塞，拥有本客户端感兴趣的片段，拥有当前需要的片段
            if peer.is_eligible() and peer.is_unchoked() and peer.am_interested() and peer.has_piece(index):
                # 加入候选列表
                ready_peers.append(peer)
        # 随机选取一个对等方
        return random.choice(ready_peers) if ready_peers else None

    # 检查是否有未将本客户端阻塞的对等方
    def has_unchoked_peers(self):
        for peer in self.peers:
            if peer.is_unchoked():
                return True
        return False

    # 计算未将本客户端阻塞的对等方的数量
    def unchoked_peers_count(self):
        cpt = 0
        for peer in self.peers:
            if peer.is_unchoked():
                cpt += 1
        return cpt

    # 从套接字中读取数据
    @staticmethod
    def _read_from_socket(sock):
        data = b''

        while True:
            try:
                # 从套接字中读取最多4096字节的数据
                buff = sock.recv(4096)
                # 如果长度小于或等于0，表示没有更多的数据可以读取或连接已关闭，退出循环
                if len(buff) <= 0:
                    break
                # 存储数据
                data += buff
            except socket.error as e:
                # 获取异常的错误号
                err = e.args[0]
                # 如果错误不是EAGAIN或EWOULDBLOCK，就打印错误
                # EAGAIN和EWOULDBLOCK表示资源暂时不可用，可以稍后重试
                if err != errno.EAGAIN or err != errno.EWOULDBLOCK:
                    logging.debug("Wrong errno {}".format(err))
                break
            except Exception:
                logging.exception("Recv failed")
                break

        return data

    # 启动线程
    def run(self):
        # 如果当前线程可运行就循环
        while self.is_active:
            # 创建一个包含所有对等方套接字的列表
            read = [peer.socket for peer in self.peers]
            # 监控套接字列表，等待可读事件。select函数在这里用于非阻塞地检查哪些套接字准备好读取数据
            read_list, _, _ = select.select(read, [], [], 1)
            # 遍历所有准备好的套接字
            for socket in read_list:
                # 根据套接字找到相应的对等端对象
                peer = self.get_peer_by_socket(socket)
                # 如果对等方状态不健康就移除
                if not peer.healthy:
                    self.remove_peer(peer)
                    continue

                try:
                    # 从套接字中读取数据
                    payload = self._read_from_socket(socket)
                except Exception as e:
                    logging.error("Recv failed %s" % e.__str__())
                    # 读取失败，移除该对等方
                    self.remove_peer(peer)
                    continue
                # 将读取到的数据追加到对等方的缓冲区
                peer.read_buffer += payload
                # 遍历从缓冲区解析出的所有消息
                for message in peer.get_messages():
                    # 按照消息类型处理每一条消息
                    self._process_new_message(message, peer)

    # 与对等方握手
    # BUG: 此方法应该被移动到tracker.Tracker._do_handshake，见tracker.Tracker.try_peer_connect
    def _do_handshake(self, peer):
        try:
            handshake = message.Handshake(self.torrent.info_hash)
            peer.send_to_peer(handshake.to_bytes())
            logging.info("new peer added : %s" % peer.ip)
            return True

        except Exception:
            logging.exception("Error when sending Handshake message")

        return False

    # 遍历从tracker.Tracker爬取的所有对等方，并尝试握手，若握手成功则加入对等方列表
    # BUG: 此方法应该删去，见tracker.Tracker.try_peer_connect
    def add_peers(self, peers):
        for peer in peers:
            if self._do_handshake(peer):
                self.peers.append(peer)
            else:
                print("Error _do_handshake")

    # 删除对等方
    def remove_peer(self, peer):
        if peer in self.peers:
            try:
                peer.socket.close()
            except Exception:
                logging.exception("")

            self.peers.remove(peer)

        #for rarest_piece in self.rarest_pieces.rarest_pieces:
        #    if peer in rarest_piece["peers"]:
        #        rarest_piece["peers"].remove(peer)

    # 根据套接字从列表中获取对等方
    def get_peer_by_socket(self, socket):
        for peer in self.peers:
            if socket == peer.socket:
                return peer

        raise Exception("Peer not present in peer_list")

    # 按照消息类型处理消息
    def _process_new_message(self, new_message: message.Message, peer: peer.Peer):
        # 如果是握手消息或保持连接消息就报错，因为这两个消息在前面已经处理过了，且只会出现一次
        if isinstance(new_message, message.Handshake) or isinstance(new_message, message.KeepAlive):
            logging.error("Handshake or KeepALive should have already been handled")
        # 处理阻塞消息
        elif isinstance(new_message, message.Choke):
            peer.handle_choke()
        # 处理非阻塞消息
        elif isinstance(new_message, message.UnChoke):
            peer.handle_unchoke()
        # 处理感兴趣消息
        elif isinstance(new_message, message.Interested):
            peer.handle_interested()
        # 处理不感兴趣消息
        elif isinstance(new_message, message.NotInterested):
            peer.handle_not_interested()
        # 处理拥有消息
        elif isinstance(new_message, message.Have):
            peer.handle_have(new_message)
        # 处理bitfield消息
        elif isinstance(new_message, message.BitField):
            peer.handle_bitfield(new_message)
        # 处理数据请求消息
        elif isinstance(new_message, message.Request):
            peer.handle_request(new_message)
        # 处理片段消息
        elif isinstance(new_message, message.Piece):
            peer.handle_piece(new_message)
        # 处理撤销消息
        elif isinstance(new_message, message.Cancel):
            peer.handle_cancel()
        # 处理端口消息
        elif isinstance(new_message, message.Port):
            peer.handle_port_request()

        else:
            logging.error("Unknown message")
