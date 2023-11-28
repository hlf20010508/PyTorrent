import logging

__author__ = 'alexisgallepe'

# 负责管理稀有片段，此版本目前并没有实装
class RarestPieces(object):
    def __init__(self, pieces_manager):

        self.pieces_manager = pieces_manager
        self.rarest_pieces = []
        # 遍历所有片段
        for piece_number in range(self.pieces_manager.number_of_pieces):
            # 将所有片段的信息存入字典并装入稀有片段列表
            self.rarest_pieces.append({"idPiece": piece_number, "numberOfPeers": 0, "peers": []})

        # pub.subscribe(self.peersBitfield, 'RarestPiece.updatePeersBitfield')

    # 通过对等方的bitfield信息更新拥有该片段的对等方数量
    # 在BitTorrent协议中，bitfield是一个用于表示对等方拥有哪些数据片段的二进制向量
    # 第0位代表第1个片段
    # 位为1表示拥有该片段，为0表示未拥有该片段
    def peers_bitfield(self, bitfield=None, peer=None, piece_index=None):
        # 如果没有更多片段则退出
        if len(self.rarest_pieces) == 0:
            raise Exception("No more piece")

        # Piece complete
        try:
            # 如果片段已经下载完成
            if not piece_index == None:
                # 从稀有片段列表中删除该片段
                self.rarest_pieces.__delitem__(piece_index)
        except Exception:
                logging.exception("Failed to remove rarest piece")

        # Peer's bitfield updated
        else:
            for i in range(len(self.rarest_pieces)):
                # 如果对等方拥有该片段且尚未被记录在稀有片段列表中
                if bitfield[i] == 1 and peer not in self.rarest_pieces[i]["peers"]:
                    # 在该片段的peers字段增加该对等方
                    self.rarest_pieces[i]["peers"].append(peer)
                    # 更新拥有该片段的对等端数量
                    self.rarest_pieces[i]["numberOfPeers"] = len(self.rarest_pieces[i]["peers"])

    # 获取按稀有度排序的片段列表
    def get_sorted_pieces(self):
        # 按照拥有该片段的对等方数量升序排序
        return sorted(self.rarest_pieces, key=lambda x: x['numberOfPeers'])
