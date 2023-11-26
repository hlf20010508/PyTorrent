__author__ = 'alexisgallepe'

from enum import Enum

# 片段中每个块的大小
BLOCK_SIZE = 2 ** 14


# 块的状态
class State(Enum):
    # 未开始下载
    FREE = 0
    # 正在下载
    PENDING = 1
    # 已下载完成
    FULL = 2


class Block():
    def __init__(self, state: State = State.FREE, block_size: int = BLOCK_SIZE, data: bytes = b'', last_seen: float = 0):
        self.state: State = state
        self.block_size: int = block_size
        # 块的真实数据
        self.data: bytes = data
        # 记录最后一次见到该数据块的时间戳，用于确定一个正在下载但长时间未更新的数据块是否应该重新标记为FREE
        self.last_seen: float = last_seen

    def __str__(self):
        return "%s - %d - %d - %d" % (self.state, self.block_size, len(self.data), self.last_seen)