import sys
from block import State

__author__ = 'alexisgallepe'

import time
import peers_manager
import pieces_manager
import torrent
import tracker
import logging
import os
import message


class Run(object):
    percentage_completed = -1
    last_log_line = ""

    def __init__(self):
        # 从命令行获取种子文件路径
        try:
            torrent_file = sys.argv[1]
        except IndexError:
            logging.error("No torrent file provided!")
            sys.exit(0)
        # 初始化
        self.torrent = torrent.Torrent().load_from_path(torrent_file)
        self.tracker = tracker.Tracker(self.torrent)

        self.pieces_manager = pieces_manager.PiecesManager(self.torrent)
        self.peers_manager = peers_manager.PeersManager(self.torrent, self.pieces_manager)

        self.peers_manager.start()
        logging.info("PeersManager Started")
        logging.info("PiecesManager Started")

    def start(self):
        # 从trackers服务器获取对等方
        peers_dict = self.tracker.get_peers_from_trackers()
        self.peers_manager.add_peers(peers_dict.values())
        # 持续循环直到所有片段下载完毕
        while not self.pieces_manager.all_pieces_completed():
            # 如果没有被阻塞的对等方就继续循环等待，直到找到对等方
            if not self.peers_manager.has_unchoked_peers():
                time.sleep(1)
                logging.info("No unchocked peers")
                continue
            # 遍历所有片段
            for piece in self.pieces_manager.pieces:
                index = piece.piece_index
                # 如果片段已下载就跳过
                if self.pieces_manager.pieces[index].is_full:
                    continue
                # 如果片段没有被下载，就随机选取一个拥有该片段的对等方
                peer = self.peers_manager.get_random_peer_having_piece(index)
                # 如果没有任何对等方拥有该片段，就继续循环等待
                if not peer:
                    continue
                # 检查片段中的每个块，如果有块的状态为正在下载但是长时间没有更新，则重置该块的状态
                self.pieces_manager.pieces[index].update_block_status()
                # 获取片段首个还没有开始下载的块，将其状态置为正在下载，并返回块的信息，包括块的偏移量
                data = self.pieces_manager.pieces[index].get_empty_block()
                # 如果没有任何块还没有开始下载，或所有块都下载完毕，就跳过此片段
                if not data:
                    continue

                piece_index, block_offset, block_length = data
                # 建立所缺块的请求信息
                piece_data = message.Request(piece_index, block_offset, block_length).to_bytes()
                # 向对等方请求所缺块，之后的下载和存储在的步骤在其他代码处实现
                peer.send_to_peer(piece_data)
            # 显示进度
            self.display_progression()

            time.sleep(0.1)

        logging.info("File(s) downloaded successfully.")
        self.display_progression()

        self._exit_threads()

    def display_progression(self):
        new_progression = 0

        for i in range(self.pieces_manager.number_of_pieces):
            for j in range(self.pieces_manager.pieces[i].number_of_blocks):
                # 加和每个片段中的每个下载完成的块的长度
                if self.pieces_manager.pieces[i].blocks[j].state == State.FULL:
                    new_progression += len(self.pieces_manager.pieces[i].blocks[j].data)

        if new_progression == self.percentage_completed:
            return

        number_of_peers = self.peers_manager.unchoked_peers_count()
        # 计算进度的百分比
        percentage_completed = float((float(new_progression) / self.torrent.total_length) * 100)

        current_log_line = "Connected peers: {} - {}% completed | {}/{} pieces".format(
            number_of_peers,
            round(percentage_completed, 2),
            self.pieces_manager.complete_pieces,
            self.pieces_manager.number_of_pieces
        )
        # 如果进度有变动就打印
        if current_log_line != self.last_log_line:
            print(current_log_line)

        self.last_log_line = current_log_line
        self.percentage_completed = new_progression

    def _exit_threads(self):
        self.peers_manager.is_active = False
        os._exit(0)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    run = Run()
    run.start()
