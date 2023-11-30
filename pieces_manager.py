__author__ = 'alexisgallepe'

import piece
import bitstring
import logging
from pubsub import pub


class PiecesManager(object):
    def __init__(self, torrent):
        self.torrent = torrent
        self.number_of_pieces = int(torrent.number_of_pieces)
        self.bitfield = bitstring.BitArray(self.number_of_pieces)
        # 片段列表初始化
        self.pieces = self._generate_pieces()
        # 加载文件信息为列表
        self.files = self._load_files()
        # 已完成的片段数量
        self.complete_pieces = 0

        # 遍历文件并将它们关联到相应的数据片段
        for file in self.files:
            id_piece = file['idPiece']
            self.pieces[id_piece].files.append(file)

        # events
        # 订阅事件，存储收到的块数据
        pub.subscribe(self.receive_block_piece, 'PiecesManager.Piece')
        # 订阅事件，在收到片段下载完成的通知后更新bitfield
        pub.subscribe(self.update_bitfield, 'PiecesManager.PieceCompleted')

    # 更新bitfield，将对应的片段置为1
    def update_bitfield(self, piece_index):
        self.bitfield[piece_index] = 1
        # BUG: 应当清空片段中的数据，否则下载下来的文件仍然存在内存中，并更改main.Client.display_progression，前往查看详情
        # self.pieces[piece_index].raw_data = b''
        # self.pieces[piece_index].blocks = []

    # 存储收到的块数据
    def receive_block_piece(self, piece):
        # 提取片段号，块在片段中的偏移量，块数据
        piece_index, piece_offset, piece_data = piece

        if self.pieces[piece_index].is_full:
            return
        # 将块数据存入片段
        self.pieces[piece_index].set_block(piece_offset, piece_data)
        # 如果片段中的块已全部下载完成
        if self.pieces[piece_index].are_all_blocks_full():
            # 设置片段状态为下载完成
            if self.pieces[piece_index].set_to_full():
                # 已完成的片段数量加1
                self.complete_pieces +=1

    # 获取块数据
    def get_block(self, piece_index, block_offset, block_length):
        for piece in self.pieces:
            if piece_index == piece.piece_index:
                # 如果片段已满，才会发送块数据
                if piece.is_full:
                    return piece.get_block(block_offset, block_length)
                else:
                    break

        return None

    # 判断是否所有片段都已下载
    def all_pieces_completed(self):
        for piece in self.pieces:
            if not piece.is_full:
                return False

        return True

    # 片段初始化
    def _generate_pieces(self):
        pieces = []
        # 最后一个片段的大小可能会小于正常大小，需要特殊处理
        last_piece = self.number_of_pieces - 1

        for i in range(self.number_of_pieces):
            # 每个片段的哈希值在种子中占20字节
            start = i * 20
            end = start + 20
            # 如果是最后一个片段
            if i == last_piece:
                # 片段大小为种子总大小减去前面片段的总大小
                piece_length = self.torrent.total_length - (self.number_of_pieces - 1) * self.torrent.piece_length
                pieces.append(piece.Piece(i, piece_length, self.torrent.pieces[start:end]))
            else:
                pieces.append(piece.Piece(i, self.torrent.piece_length, self.torrent.pieces[start:end]))

        return pieces

    # 处理文件信息
    def _load_files(self):
        files = []
        # 当前处理的总数据片段的偏移量
        piece_offset = 0
        # 已使用的片段大小
        piece_size_used = 0
        # 根据文件名遍历
        for f in self.torrent.file_names:
            # 文件大小
            current_size_file = f["length"]
            # 文件中的偏移量
            file_offset = 0
            # 如果当前文件还有未处理完的部分就继续循环
            while current_size_file > 0:
                # 计算当前偏移量对应的片段号
                id_piece = int(piece_offset / self.torrent.piece_length)
                # 计算当前片段中剩余的可用大小
                piece_size = self.pieces[id_piece].piece_size - piece_size_used
                # 如果文件剩余部分小于当前片段的剩余部分，片段没有被用完。这是文件的最后一部分
                if current_size_file - piece_size < 0:
                    # 记录文件信息
                    file = {"length": current_size_file,
                            "idPiece": id_piece,
                            "fileOffset": file_offset,
                            "pieceOffset": piece_size_used,
                            "path": f["path"]
                            }
                    # 更新偏移量
                    piece_offset += current_size_file
                    file_offset += current_size_file
                    # 记录片段已被使用的大小
                    piece_size_used += current_size_file
                    current_size_file = 0
                # 这是文件的普通部分，片段会被用完
                else:
                    # 剩余文件大小
                    current_size_file -= piece_size
                    # 记录文件信息
                    file = {"length": piece_size,
                            "idPiece": id_piece,
                            "fileOffset": file_offset,
                            "pieceOffset": piece_size_used,
                            "path": f["path"]
                            }
                    # 更新偏移量
                    piece_offset += piece_size
                    file_offset += piece_size
                    # 片段已被用完，重置为0
                    piece_size_used = 0

                files.append(file)
        return files
