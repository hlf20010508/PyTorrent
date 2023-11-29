import time

__author__ = 'alexisgallepe'

import socket
import struct
import bitstring
from pubsub import pub
import logging

import message


class Peer(object):
    def __init__(self, number_of_pieces, ip, port=6881):
        # 上次与该对等方通信的时间
        self.last_call = 0.0
        # 是否已经握手过
        self.has_handshaked = False
        # 该对等方的状态是否正常
        self.healthy = False
        # 存储从对等方接收的数据
        self.read_buffer = b''
        # 表示与对等方的网络连接
        self.socket = None
        self.ip = ip
        self.port = port
        # 种子中的片段数量
        self.number_of_pieces = number_of_pieces
        # 初始化bitfield，全部置为0
        self.bit_field = bitstring.BitArray(number_of_pieces)
        # 对等方状态
        self.state = {
            'am_choking': True,
            'am_interested': False,
            'peer_choking': True,
            'peer_interested': False,
        }

    def __hash__(self):
        return "%s:%d" % (self.ip, self.port)

    # 本客户端与该对等方连接
    def connect(self):
        try:
            self.socket = socket.create_connection((self.ip, self.port), timeout=2)
            self.socket.setblocking(False)
            logging.debug("Connected to peer ip: {} - port: {}".format(self.ip, self.port))
            self.healthy = True

        except Exception as e:
            print("Failed to connect to peer (ip: %s - port: %s - %s)" % (self.ip, self.port, e.__str__()))
            return False

        return True

    # 向对等方发送消息
    def send_to_peer(self, msg):
        try:
            self.socket.send(msg)
            self.last_call = time.time()
        except Exception as e:
            self.healthy = False
            logging.error("Failed to send to peer : %s" % e.__str__())

    # 判断对等方是否能够随机选择算法被选中以接收片段
    # 防止同一个对等方被连续要求发送片段
    def is_eligible(self):
        now = time.time()
        return (now - self.last_call) > 0.2

    # 通过bitfield判断该对等方是否拥有该片段
    def has_piece(self, index):
        return self.bit_field[index]

    # 获取本客户端是否将对等方设置为阻塞状态
    def am_choking(self):
        return self.state['am_choking']

    # 获取本客户端是否将对等方设置为非阻塞状态
    def am_unchoking(self):
        return not self.am_choking()

    # 获取对等方是否将本客户端设置为阻塞状态
    def is_choking(self):
        return self.state['peer_choking']

    # 获取对等方是否将本客户端设置为非阻塞状态
    def is_unchoked(self):
        return not self.is_choking()

    # 获取对等方是否对本客户端拥有的片段感兴趣
    def is_interested(self):
        return self.state['peer_interested']

    # 获取本客户端是否对对等方拥有的片段感兴趣
    def am_interested(self):
        return self.state['am_interested']

    # 设置对等方已将本客户端设置为阻塞
    def handle_choke(self):
        logging.debug('handle_choke - %s' % self.ip)
        self.state['peer_choking'] = True

    # 设置对等方已将本客户端设置为非阻塞
    def handle_unchoke(self):
        logging.debug('handle_unchoke - %s' % self.ip)
        self.state['peer_choking'] = False

    # 设置对等方对本客户端拥有的片段感兴趣
    def handle_interested(self):
        logging.debug('handle_interested - %s' % self.ip)
        self.state['peer_interested'] = True
        # 如果本客户端已将该对等方设置为阻塞，就将解除阻塞的消息发送给对等端，表示我们现在愿意开始或继续向该对等端发送数据
        if self.am_choking():
            # 不知道为什么没有将self.state['am_choking']更改为False
            # 不过程序本身在发送给对等方数据前就不检查自己有没有阻塞该对等方，应该还没有实现
            unchoke = message.UnChoke().to_bytes()
            self.send_to_peer(unchoke)

    # 设置对等方对本客户端拥有的片段不感兴趣
    def handle_not_interested(self):
        logging.debug('handle_not_interested - %s' % self.ip)
        self.state['peer_interested'] = False

    # 处理对等方发送的它所拥有的片段信息
    def handle_have(self, have):
        """
        :type have: message.Have
        """
        logging.debug('handle_have - ip: %s - piece: %s' % (self.ip, have.piece_index))
        # 更新对等方的bitfiled，将对等方表明的其所拥有的片段在bitfield中设置为1
        self.bit_field[have.piece_index] = True
        # 如果该对等方把本客户端阻塞，且原本我们对该对等方没有兴趣
        if self.is_choking() and not self.state['am_interested']:
            interested = message.Interested().to_bytes()
            # 告诉对等方我们对它有兴趣，请求不要阻塞我们
            self.send_to_peer(interested)
            # 现在我们对该对等方有兴趣
            self.state['am_interested'] = True

        # pub.sendMessage('RarestPiece.updatePeersBitfield', bitfield=self.bit_field)

    # 处理收到的bitfield信息
    def handle_bitfield(self, bitfield):
        """
        :type bitfield: message.BitField
        """
        logging.debug('handle_bitfield - %s - %s' % (self.ip, bitfield.bitfield))
        # 更新该对等方的bitfield信息
        self.bit_field = bitfield.bitfield

        if self.is_choking() and not self.state['am_interested']:
            interested = message.Interested().to_bytes()
            self.send_to_peer(interested)
            self.state['am_interested'] = True

        # pub.sendMessage('RarestPiece.updatePeersBitfield', bitfield=self.bit_field)

    # 处理对等方向本客户端发送的数据请求
    def handle_request(self, request):
        """
        :type request: message.Request
        """
        logging.debug('handle_request - %s' % self.ip)
        # 如果对等方对我们有兴趣，且没有阻塞我们，就将其想要的片段发送过去
        if self.is_interested() and self.is_unchoked():
            # BUG: 此处应该为PeersManager.PeerRequestsPiece
            pub.sendMessage('PiecesManager.PeerRequestsPiece', request=request, peer=self)

    # 处理对等方发送过来的片段信息
    def handle_piece(self, message):
        """
        :type message: message.Piece
        """
        # 存储片段信息中的块
        pub.sendMessage('PiecesManager.Piece', piece=(message.piece_index, message.block_offset, message.block))

    # 处理对等方发送的取消请求（未实现），撤销之前发出的数据块请求
    def handle_cancel(self):
        logging.debug('handle_cancel - %s' % self.ip)

    # 处理对等方发送的端口号信息（未实现），记录它提供的端口号
    def handle_port_request(self):
        logging.debug('handle_port_request - %s' % self.ip)

    # 处理握手消息
    def _handle_handshake(self):
        try:
            # 从缓冲区解析握手消息
            handshake_message = message.Handshake.from_bytes(self.read_buffer)
            self.has_handshaked = True
            # 更新缓冲区，移除已处理的握手消息部分
            self.read_buffer = self.read_buffer[handshake_message.total_length:]
            logging.debug('handle_handshake - %s' % self.ip)
            return True

        except Exception:
            logging.exception("First message should always be a handshake message")
            self.healthy = False

        return False

    # 处理保持连接活跃的消息
    def _handle_keep_alive(self):
        try:
            # 从缓冲区解析保持连接活跃的消息
            keep_alive = message.KeepAlive.from_bytes(self.read_buffer)
            logging.debug('handle_keep_alive - %s' % self.ip)
        # 如果收到的不是保持连接活跃的消息就返回False
        except message.WrongMessageException:
            return False
        except Exception:
            logging.exception("Error KeepALive, (need at least 4 bytes : {})".format(len(self.read_buffer)))
            return False
        # 移除已处理的保持连接活跃消息部分
        self.read_buffer = self.read_buffer[keep_alive.total_length:]
        return True

    # 从缓冲区中提取消息并处理
    def get_messages(self):
        # 只要读缓冲区中的数据长度超过4字节且对等端健康状态为正常就循环
        while len(self.read_buffer) > 4 and self.healthy:
            # 如果尚未完成握手，则尝试处理握手消息。如果已经握手，则尝试处理保持连接活跃的消息。
            if (not self.has_handshaked and self._handle_handshake()) or self._handle_keep_alive():
                continue
            # 解析读缓冲区前4字节以获取消息的有效载荷长度
            payload_length, = struct.unpack(">I", self.read_buffer[:4])
            # 计算消息的总长度，包括前缀
            total_length = payload_length + 4
            # 如果读缓冲区中的数据不足以构成完整的消息，跳出循环
            if len(self.read_buffer) < total_length:
                break
            else:
                # 获取消息的完整字节序列
                payload = self.read_buffer[:total_length]
                # 更新读缓冲区，移除已提取的消息部分
                self.read_buffer = self.read_buffer[total_length:]

            try:
                # 解析消息并分发给相应的处理函数
                received_message = message.MessageDispatcher(payload).dispatch()
                # 如果成功解析出消息，则返回消息
                if received_message:
                    yield received_message
            except message.WrongMessageException as e:
                logging.exception(e.__str__())
