__author__ = 'alexisgallepe'

import hashlib
import math
import time
import logging

from pubsub import pub
from block import Block, BLOCK_SIZE, State


class Piece(object):
    def __init__(self, piece_index: int, piece_size: int, piece_hash: str):
        # 片段号
        self.piece_index: int = piece_index
        # 片段大小
        self.piece_size: int = piece_size
        # 片段哈希值
        self.piece_hash: str = piece_hash
        # 片段是否已完整下载
        self.is_full: bool = False
        # 存储与该片段相关联的文件信息
        self.files = []
        # 该片段的原始数据
        self.raw_data: bytes = b''
        # 该片段包含的块数量
        self.number_of_blocks: int = int(math.ceil(float(piece_size) / BLOCK_SIZE))
        # 该片段中所包含的块列表
        self.blocks: list[Block] = []
        # 初始化片段
        self._init_blocks()

    # 如果数据块在一段时间内处于挂起状态，则重置数据块状态
    def update_block_status(self):  # if block is pending for too long : set it free
        for i, block in enumerate(self.blocks):
            # 如果数据块挂起时间超过5秒则重置数据块
            if block.state == State.PENDING and (time.time() - block.last_seen) > 5:
                self.blocks[i] = Block()

    # 根据偏移量设置数据块的内容
    def set_block(self, offset, data):
        # 计算块在片段中的编号
        index = int(offset / BLOCK_SIZE)
        # 如果片段未下载完成且当前块未下载完成，则存储收到的块数据并设置块状态为下载完成
        if not self.is_full and not self.blocks[index].state == State.FULL:
            self.blocks[index].data = data
            self.blocks[index].state = State.FULL

    # 根据偏移量和长度获取数据块内容
    def get_block(self, block_offset, block_length):
        # BUG: 应当为
        # return self.raw_data[block_offset:block_offset + block_length]
        return self.raw_data[block_offset:block_length]
        # BUG: 由于片段下载完后应当清空数据，这样会导致无法得到数据，详见pieces_manager.PiecesManager.update_bitfield
        # 因此必须要实现从本地文件读取数据
        # file_data_list = []
        # for file in self.files:
        #     # 文件路径
        #     path_file = file["path"]
        #     # 该片段中的文件数据在该文件中的偏移量
        #     file_offset = file["fileOffset"]
        #     # 该文件在该片段中的偏移量
        #     piece_offset = file["pieceOffset"]
        #     # 要写入的数据长度
        #     length = file["length"]

        #     try:
        #         # 打开文件
        #         f = open(path_file, 'rb')
        #     except Exception:
        #         logging.exception("Can't read file %s" % path_file)
        #         return
        #     # 将文件光标指向文件偏移量
        #     f.seek(file_offset)
        #     # 读取数据
        #     data = f.read(length)
        #     file_data_list.append((piece_offset, data))
        #     f.close()
        # # 根据偏移量升序排序
        # file_data_list.sort(key=lambda x: x[0])
        # # 将数据拼接成片段
        # piece = b''.join([data for _, data in file_data_list])
        # # 返回指定的块
        # return piece[block_offset : block_offset + block_length]

    # 获取一个未被占用的数据块信息
    def get_empty_block(self):
        if self.is_full:
            return None

        for block_index, block in enumerate(self.blocks):
            if block.state == State.FREE:
                self.blocks[block_index].state = State.PENDING
                self.blocks[block_index].last_seen = time.time()
                return self.piece_index, block_index * BLOCK_SIZE, block.block_size

        return None

    # 检查所有数据块是否已满，以判断整个片段是否下载完成
    def are_all_blocks_full(self):
        for block in self.blocks:
            if block.state == State.FREE or block.state == State.PENDING:
                return False

        return True

    # 设置片段状态为已下载
    def set_to_full(self):
        # 合并所有块
        data = self._merge_blocks()
        # 若计算出的片段哈希值与记录不相同，则重置片段，重新下载
        if not self._valid_blocks(data):
            self._init_blocks()
            return False

        self.is_full = True
        self.raw_data = data
        # 将片段写入磁盘
        self._write_piece_on_disk()
        # 通知片段管理器更新bitfield
        pub.sendMessage('PiecesManager.PieceCompleted', piece_index=self.piece_index)

        return True

    # 初始化片段
    def _init_blocks(self):
        # 将块列表清空
        self.blocks = []
        # 增加空的块对象，如果块数量大于1则遍历增加
        if self.number_of_blocks > 1:
            for i in range(self.number_of_blocks):
                self.blocks.append(Block())

            # Last block of last piece, the special block
            # 如果块是片段的最后一个块，其大小可能会小于普通块的大小，通过计算余数得到该块正确的大小
            if (self.piece_size % BLOCK_SIZE) > 0:
                self.blocks[self.number_of_blocks - 1].block_size = self.piece_size % BLOCK_SIZE
        # 如果只有1个块，则直接增加，块大小就是片段大小
        else:
            self.blocks.append(Block(block_size=int(self.piece_size)))

    # 将片段数据写入磁盘
    def _write_piece_on_disk(self):
        # 遍历片段中包含的文件
        for file in self.files:
            # 文件路径
            path_file = file["path"]
            # 该片段中的文件数据在该文件中的偏移量
            file_offset = file["fileOffset"]
            # 该文件在该片段中的偏移量
            piece_offset = file["pieceOffset"]
            # 要写入的数据长度
            length = file["length"]

            try:
                # 尝试部分覆盖写入
                # 有别于a+，r+在seek移动光标时，若光标超过当前文件长度，会在中间填充空比特，当光标在文件长度内，则会覆盖写入
                # 这样即使片段写入的顺序不一样，也不会出现最终顺序乱的情况
                f = open(path_file, 'r+b')  # Already existing file
            except IOError:
                # 无法追加写入，说明文件还未被创建，则使用覆盖写入
                f = open(path_file, 'wb')  # New file
            except Exception:
                logging.exception("Can't write to file")
                return
            # 将文件光标指向文件偏移量
            f.seek(file_offset)
            # 写入数据
            f.write(self.raw_data[piece_offset:piece_offset + length])
            f.close()

    # 拼接所有块的数据
    def _merge_blocks(self):
        buf = b''

        for block in self.blocks:
            buf += block.data

        return buf

    # 计算片段哈希值，检查是否匹配
    def _valid_blocks(self, piece_raw_data):
        hashed_piece_raw_data = hashlib.sha1(piece_raw_data).digest()

        if hashed_piece_raw_data == self.piece_hash:
            return True

        logging.warning("Error Piece Hash")
        logging.debug("{} : {}".format(hashed_piece_raw_data, self.piece_hash))
        return False
