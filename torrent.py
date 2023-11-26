import math

__author__ = 'alexisgallepe'

import hashlib
import time
from bcoding import bencode, bdecode
import logging
import os


class Torrent(object):
    def __init__(self):
        # 用于记录导入的种子文件的全部信息
        self.torrent_file = {}
        # 种子总大小
        self.total_length: int = 0
        # 每个片段的长度
        self.piece_length: int = 0
        # 片段的哈希值
        self.pieces: int = 0
        # 种子的哈希值
        self.info_hash: str = ''
        # 用于存储本客户端生成的peer id
        self.peer_id: str = ''
        # trackers地址列表
        self.announce_list = ''
        # 种子中所有文件的文件路径
        self.file_names = []
        # 片段数量
        self.number_of_pieces: int = 0

    def load_from_path(self, path):
        # 读取文件并编码为字典
        with open(path, 'rb') as file:
            contents = bdecode(file)

        self.torrent_file = contents
        # 提取信息
        self.piece_length = self.torrent_file['info']['piece length']
        self.pieces = self.torrent_file['info']['pieces']
        # 将info信息重新编码为bencode格式
        raw_info_hash = bencode(self.torrent_file['info'])
        # 使用sha1计算raw_info_hash的哈希值，并保存为字节字符串，用作种子文件的唯一标识
        self.info_hash = hashlib.sha1(raw_info_hash).digest()
        # 为本客户端基于当前时间生成peer id
        self.peer_id = self.generate_peer_id()
        # 从种子文件信息中获取trackers地址列表
        self.announce_list = self.get_trakers()
        # 初始化文件系统，创建文件目录，并存储文件路径
        self.init_files()
        # 计算片段数量
        self.number_of_pieces = math.ceil(self.total_length / self.piece_length)
        logging.debug(self.announce_list)
        logging.debug(self.file_names)
        # 检查种子中的文件大小和文件数量，如果为0则触发异常
        assert(self.total_length > 0)
        assert(len(self.file_names) > 0)

        return self

    def init_files(self):
        # 获取种子中根目录的路径
        root = self.torrent_file['info']['name']
        # 如果有files字段，则表明种子中包含多个文件
        if 'files' in self.torrent_file['info']:
            if not os.path.exists(root):
                # 创建根目录，并定义其权限为没有特殊权限设定（0），所有者读写执行（4+2+1），用户和访客读写（4+2）
                os.mkdir(root, 0o0766 )
            # 遍历根目录下的所有文件路径
            for file in self.torrent_file['info']['files']:
                # 将文件路径的各个部分同根目录拼接起来
                # file["path"]的结构形如["music", "song.mp3"]
                path_file = os.path.join(root, *file["path"])
                # 检查该文件的父路径是否存在，若不存在则创建该父路径
                if not os.path.exists(os.path.dirname(path_file)):
                    os.makedirs(os.path.dirname(path_file))
                # 存储文件路径，并附上文件大小
                self.file_names.append({"path": path_file , "length": file["length"]})
                # 更新种子的总大小
                self.total_length += file["length"]
        # 若没有files字段，说明种子中只有一个文件
        else:
            self.file_names.append({"path": root , "length": self.torrent_file['info']['length']})
            self.total_length = self.torrent_file['info']['length']

    def get_trakers(self):
        if 'announce-list' in self.torrent_file:
            return self.torrent_file['announce-list']
        else:
            return [[self.torrent_file['announce']]]

    def generate_peer_id(self):
        seed = str(time.time())
        return hashlib.sha1(seed.encode('utf-8')).digest()
