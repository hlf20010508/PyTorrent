import logging
import random
import socket
from struct import pack, unpack

# HandShake - String identifier of the protocol for BitTorrent V1
import bitstring

# 协议标识
HANDSHAKE_PSTR_V1 = b"BitTorrent protocol"
# 协议名称长度
HANDSHAKE_PSTR_LEN = len(HANDSHAKE_PSTR_V1)
# 可能表示消息前缀的长度，没有实装，意义不明
LENGTH_PREFIX = 4

# 用于抛出消息类型错误的异常
class WrongMessageException(Exception):
    pass

# 负责分派不同类型的BitTorrent消息
class MessageDispatcher:

    def __init__(self, payload):
        # 原始消息
        self.payload = payload

    def dispatch(self):
        try:
            # 获取原始消息的长度和消息类型
            payload_length, message_id, = unpack(">IB", self.payload[:5])
        except Exception as e:
            logging.warning("Error when unpacking message : %s" % e.__str__())
            return None
        # 定义消息类型字典，用于映射各个消息类型对应的消息类
        map_id_to_message = {
            0: Choke,
            1: UnChoke,
            2: Interested,
            3: NotInterested,
            4: Have,
            5: BitField,
            6: Request,
            7: Piece,
            8: Cancel,
            9: Port
        }
        # 若收到的消息类型不再消息类型字典中则抛出异常
        if message_id not in list(map_id_to_message.keys()):
            raise WrongMessageException("Wrong message id")

        return map_id_to_message[message_id].from_bytes(self.payload)

# 消息类型的基类
class Message:
    # 在子类中实现序列化逻辑
    def to_bytes(self):
        raise NotImplementedError()
    # 在子类中实现反序列化逻辑
    @classmethod
    def from_bytes(cls, payload):
        raise NotImplementedError()


"""
UDP Tracker
"""

# UDP动作类型
# 1. connect
# 动作ID：0
# 描述：用于与追踪器建立连接的请求。客户端首先发送连接请求以获取一个有效的连接ID，用于后续的通信
# 2. announce
# 动作ID：1
# 描述：一旦建立了连接，客户端发送宣告请求，以通知追踪器其正在下载或上传的torrent信息，并获取其他对等端（peers）的信息
# 3. scrape
# 动作ID：2
# 描述：用于获取特定torrent的统计信息，如完成下载的对等端数目、当前做种的对等端数目等
# 4. error
# 动作ID：3
# 描述：追踪器响应中的错误消息。如果请求失败，追踪器会发送包含错误信息的响应
# 5. announce_response
# 动作ID：通常隐含于响应中
# 描述：追踪器对宣告请求的响应。包含了其他对等端的信息，以及与torrent相关的统计数据
# 6. scrape_response
# 动作ID：通常隐含于响应中
# 描述：追踪器对刮擦请求的响应。包含了请求的torrent的统计信息

# 用于创建和处理UDP tracker连接请求
class UdpTrackerConnection(Message):
    """
        connect = <connection_id><action><transaction_id>
            - connection_id = 64-bit integer
            - action = 32-bit integer
            - transaction_id = 32-bit integer

        Total length = 64 + 32 + 32 = 128 bytes
    """

    def __init__(self):
        super(UdpTrackerConnection, self).__init__()
        # 连接ID
        # >表示大端字节序，Q表示8字节无符号长整型
        # 0x41727101980在BitTorrent UDP协议中是一个常量，用来在初次连接中表明自己遵循BitTorrent协议
        self.conn_id = pack('>Q', 0x41727101980)
        # 动作类型
        # I表示4字节无符号整型
        self.action = pack('>I', 0)
        # 事务ID，每次都随机，保证唯一性
        self.trans_id = pack('>I', random.randint(0, 100000))
    # 将信息拼接成字节串
    def to_bytes(self):
        return self.conn_id + self.action + self.trans_id
    # 从字节串中提取信息
    def from_bytes(self, payload):
        self.action, = unpack('>I', payload[:4])
        self.trans_id, = unpack('>I', payload[4:8])
        self.conn_id, = unpack('>Q', payload[8:])

# 用于创建和处理UDP tracker宣告请求
class UdpTrackerAnnounce(Message):
    """
        connect = <connection_id><action><transaction_id>

        0	64-bit integer	connection_id
8	32-bit integer	action	1
12	32-bit integer	transaction_id
16	20-byte string	info_hash
36	20-byte string	peer_id
56	64-bit integer	downloaded
64	64-bit integer	left
72	64-bit integer	uploaded
80	32-bit integer	event
84	32-bit integer	IP address	0
88	32-bit integer	key
92	32-bit integer	num_want	-1
96	16-bit integer	port

            - connection_id = 64-bit integer
            - action = 32-bit integer
            - transaction_id = 32-bit integer

        Total length = 64 + 32 + 32 = 128 bytes
    """

    def __init__(self, info_hash, conn_id, peer_id):
        super(UdpTrackerAnnounce, self).__init__()
        # 本客户端的peer id
        self.peer_id = peer_id
        # 从UdpTrackerConnection得到的连接ID
        self.conn_id = conn_id
        # 种子哈希值
        self.info_hash = info_hash
        # 事务ID
        self.trans_id = pack('>I', random.randint(0, 100000))
        # 动作类型
        self.action = pack('>I', 1)

    def to_bytes(self):
        conn_id = pack('>Q', self.conn_id)
        action = self.action
        trans_id = self.trans_id
        # 已下载的字节数，初始为0
        downloaded = pack('>Q', 0)
        # 还需要下载的字节数，初始为0
        left = pack('>Q', 0)
        # 已上传的字节数，初始为0
        uploaded = pack('>Q', 0)
        # 初始状态，0 表示“无事件”，1 表示“完成下载”，2 表示“开始下载”，3 表示“停止下载”
        event = pack('>I', 0)
        # 指定本客户端的IP地址。0表示客户端希望tracker自动检测其IP地址
        ip = pack('>I', 0)
        # 用作本客户端的标识符，但应当使用随机生成的值以保证唯一性
        key = pack('>I', 0)
        # 本客户端请求的对等方数量，-1表示尽可能多或者由tracker决定
        # i表示4字节有符号整型
        num_want = pack('>i', -1)
        # 本客户端的监听端口号
        # h表示2字节短整型
        port = pack('>h', 8000)

        msg = (conn_id + action + trans_id + self.info_hash + self.peer_id + downloaded +
               left + uploaded + event + ip + key + num_want + port)

        return msg

# 用于解析UDP tracker宣告响应
class UdpTrackerAnnounceOutput:
    """
        connect = <connection_id><action><transaction_id>

0	32-bit integer	action	1
4	32-bit integer	transaction_id
8	32-bit integer	interval
12	32-bit integer	leechers
16	32-bit integer	seeders
20 + 6 * n	32-bit integer	IP address
24 + 6 * n	16-bit integer	TCP port
20 + 6 * N

    """

    def __init__(self):
        # 动作类型
        self.action = None
        # 事务ID
        self.transaction_id = None
        # 表示客户端应该等待多长时间（秒）才能再次发送宣告。这是tracker用来控制宣告请求频率的一种方式
        self.interval = None
        # 只下载而不上传，或上传下载比例很小，或者有其他不良行为的客户端数量
        self.leechers = None
        # 做种者数
        self.seeders = None
        # 对等方地址列表
        self.list_sock_addr = []

    def from_bytes(self, payload):
        self.action, = unpack('>I', payload[:4])
        self.transaction_id, = unpack('>I', payload[4:8])
        self.interval, = unpack('>I', payload[8:12])
        self.leechers, = unpack('>I', payload[12:16])
        self.seeders, = unpack('>I', payload[16:20])
        self.list_sock_addr = self._parse_sock_addr(payload[20:])

    # 从字节串解析对等方地址为列表
    def _parse_sock_addr(self, raw_bytes):
        socks_addr = []

        # socket address : <IP(4 bytes)><Port(2 bytes)>
        # len(socket addr) == 6 bytes
        # 遍历原始字节，每6个字节代表一个对等方地址，4位IP地址，2位端口号
        for i in range(int(len(raw_bytes) / 6)):
            # 当前对等方地址的开始下标
            start = i * 6
            # 结束下标
            end = start + 6
            # 提取IP地址
            ip = socket.inet_ntoa(raw_bytes[start:(end - 2)])
            # 提取原始端口号数据
            raw_port = raw_bytes[(end - 2):end]
            # 将2字节端口号转换为整数。由于端口号是大端序，所以第2个字节是低位，第1个字节是高位。
            port = raw_port[1] + raw_port[0] * 256

            socks_addr.append((ip, port))

        return socks_addr


"""
    Bittorrent messages
"""

# 握手消息
class Handshake(Message):
    """
        Handshake = <pstrlen><pstr><reserved><info_hash><peer_id>
            - pstrlen = length of pstr (1 byte)
            - pstr = string identifier of the protocol: "BitTorrent protocol" (19 bytes)
            - reserved = 8 reserved bytes indicating extensions to the protocol (8 bytes)
            - info_hash = hash of the value of the 'info' key of the torrent file (20 bytes)
            - peer_id = unique identifier of the Peer (20 bytes)

        Total length = payload length = 49 + len(pstr) = 68 bytes (for BitTorrent v1)
    """
    # 原始消息总长度为68字节
    payload_length = 68
    total_length = payload_length

    def __init__(self, info_hash, peer_id=b'-ZZ0007-000000000000'):
        super(Handshake, self).__init__()
        # 确保种子哈希值长度为20字节，这是BitTorrent协议的要求
        assert len(info_hash) == 20
        # 确保peer id长度小于255字节，不会太长
        assert len(peer_id) < 255
        self.peer_id = peer_id
        self.info_hash = info_hash

    def to_bytes(self):
        # 创建8个字节的保留字段，初始化为零
        reserved = b'\x00' * 8
        # 将握手消息序列化为字节序列
        # B标识1字节无符号字节
        # B将HANDSHAKE_PSTR_LEN打包为1个字节
        # {}s用来打包HANDSHAKE_PSTR_V1，长度为HANDSHAKE_PSTR_LEN字节
        # 8s用来打包reserved，长度为8字节
        # 两个20s分别用来打包self.info_hash和self.peer_id，长度为20字节
        # 当peer id长度小于20字节时会填充空字节，大于时会截断
        handshake = pack(">B{}s8s20s20s".format(HANDSHAKE_PSTR_LEN),
                         HANDSHAKE_PSTR_LEN,
                         HANDSHAKE_PSTR_V1,
                         reserved,
                         self.info_hash,
                         self.peer_id)

        return handshake

    @classmethod
    def from_bytes(cls, payload):
        # 从第1个字节解析协议标识符长度
        pstrlen, = unpack(">B", payload[:1])
        # 在剩下的字节中解析协议标识符、保留字段、种子哈希值和对等方peer id
        pstr, reserved, info_hash, peer_id = unpack(">{}s8s20s20s".format(pstrlen), payload[1:cls.total_length])
        # 若协议名称不符则抛出异常
        if pstr != HANDSHAKE_PSTR_V1:
            raise ValueError("Invalid string identifier of the protocol")

        return Handshake(info_hash, peer_id)

# 保持活跃
class KeepAlive(Message):
    """
        KEEP_ALIVE = <length>
            - payload length = 0 (4 bytes)
    """
    # 有效载荷为0，因为不包含任何信息
    payload_length = 0
    # 总长度为4字节，即4字节的0
    total_length = 4

    def __init__(self):
        super(KeepAlive, self).__init__()

    def to_bytes(self):
        return pack(">I", self.payload_length)

    @classmethod
    def from_bytes(cls, payload):
        payload_length = unpack(">I", payload[:cls.total_length])

        if payload_length != 0:
            raise WrongMessageException("Not a Keep Alive message")

        return KeepAlive()

# 告知对等方本自己被阻塞
class Choke(Message):
    """
        CHOKE = <length><message_id>
            - payload length = 1 (4 bytes)
            - message id = 0 (1 byte)
    """
    message_id = 0
    chokes_me = True
    # 有效载荷为1字节，仅包含消息类型
    payload_length = 1
    # 4字节表示有效载荷字节数，1字节表示消息类型
    total_length = 5

    def __init__(self):
        super(Choke, self).__init__()

    def to_bytes(self):
        return pack(">IB", self.payload_length, self.message_id)

    @classmethod
    def from_bytes(cls, payload):
        payload_length, message_id = unpack(">IB", payload[:cls.total_length])
        if message_id != cls.message_id:
            raise WrongMessageException("Not a Choke message")

        return Choke()

# 告知对等方自己已解除阻塞
class UnChoke(Message):
    """
        UnChoke = <length><message_id>
            - payload length = 1 (4 bytes)
            - message id = 1 (1 byte)
    """
    message_id = 1
    chokes_me = False

    payload_length = 1
    total_length = 5

    def __init__(self):
        super(UnChoke, self).__init__()

    def to_bytes(self):
        return pack(">IB", self.payload_length, self.message_id)

    @classmethod
    def from_bytes(cls, payload):
        payload_length, message_id = unpack(">IB", payload[:cls.total_length])

        if message_id != cls.message_id:
            raise WrongMessageException("Not an UnChoke message")

        return UnChoke()

# 告知对等方，其拥有的某些数据片段是自己需要的
class Interested(Message):
    """
        INTERESTED = <length><message_id>
            - payload length = 1 (4 bytes)
            - message id = 2 (1 byte)
    """
    message_id = 2
    interested = True

    payload_length = 1
    total_length = 4 + payload_length

    def __init__(self):
        super(Interested, self).__init__()

    def to_bytes(self):
        return pack(">IB", self.payload_length, self.message_id)

    @classmethod
    def from_bytes(cls, payload):
        payload_length, message_id = unpack(">IB", payload[:cls.total_length])

        if message_id != cls.message_id:
            raise WrongMessageException("Not an Interested message")

        return Interested()

# 告知对等方，其拥有的所有数据片段都是自己不需要的
class NotInterested(Message):
    """
        NOT INTERESTED = <length><message_id>
            - payload length = 1 (4 bytes)
            - message id = 3 (1 byte)
    """
    message_id = 3
    interested = False

    payload_length = 1
    total_length = 5

    def __init__(self):
        super(NotInterested, self).__init__()

    def to_bytes(self):
        return pack(">IB", self.payload_length, self.message_id)

    @classmethod
    def from_bytes(cls, payload):
        payload_length, message_id = unpack(">IB", payload[:cls.total_length])
        if message_id != cls.message_id:
            raise WrongMessageException("Not a Non Interested message")

        return Interested()

# 向所有连接的对等方发送消息，告知他们自己已有了某个片段
class Have(Message):
    """
        HAVE = <length><message_id><piece_index>
            - payload length = 5 (4 bytes)
            - message_id = 4 (1 byte)
            - piece_index = zero based index of the piece (4 bytes)
    """
    message_id = 4
    # 1字节的消息类型，4字节的片段号
    payload_length = 5
    total_length = 4 + payload_length

    def __init__(self, piece_index):
        super(Have, self).__init__()
        # 要告知的片段号
        self.piece_index = piece_index

    def to_bytes(self):
        pack(">IBI", self.payload_length, self.message_id, self.piece_index)

    @classmethod
    def from_bytes(cls, payload):
        payload_length, message_id, piece_index = unpack(">IBI", payload[:cls.total_length])
        if message_id != cls.message_id:
            raise WrongMessageException("Not a Have message")

        return Have(piece_index)

# 位场信息，一个字节表示一个片段，1表示拥有该片段，0表示没有该片段
# 用于告知对等方自己拥有哪些片段
class BitField(Message):
    """
        BITFIELD = <length><message id><bitfield>
            - payload length = 1 + bitfield_size (4 bytes)
            - message id = 5 (1 byte)
            - bitfield = bitfield representing downloaded pieces (bitfield_size bytes)
    """
    message_id = 5

    # Unknown until given a bitfield
    # 在获得位场信息后才能确定具体的长度
    payload_length = -1
    total_length = -1

    def __init__(self, bitfield):  # bitfield is a bitstring.BitArray
        super(BitField, self).__init__()
        self.bitfield = bitfield
        # 转换为字节序列
        self.bitfield_as_bytes = bitfield.tobytes()
        # 计算长度
        self.bitfield_length = len(self.bitfield_as_bytes)
        # 更新长度信息
        self.payload_length = 1 + self.bitfield_length
        self.total_length = 4 + self.payload_length

    def to_bytes(self):
        return pack(">IB{}s".format(self.bitfield_length),
                    self.payload_length,
                    self.message_id,
                    self.bitfield_as_bytes)

    @classmethod
    def from_bytes(cls, payload):
        payload_length, message_id = unpack(">IB", payload[:5])
        bitfield_length = payload_length - 1

        if message_id != cls.message_id:
            raise WrongMessageException("Not a BitField message")

        raw_bitfield, = unpack(">{}s".format(bitfield_length), payload[5:5 + bitfield_length])
        bitfield = bitstring.BitArray(bytes=bytes(raw_bitfield))

        return BitField(bitfield)

# 用于请求下载特定的数据块
class Request(Message):
    """
        REQUEST = <length><message id><piece index><block offset><block length>
            - payload length = 13 (4 bytes)
            - message id = 6 (1 byte)
            - piece index = zero based piece index (4 bytes)
            - block offset = zero based of the requested block (4 bytes)
            - block length = length of the requested block (4 bytes)
    """
    message_id = 6

    payload_length = 13
    total_length = 4 + payload_length

    def __init__(self, piece_index, block_offset, block_length):
        super(Request, self).__init__()
        # 片段号
        self.piece_index = piece_index
        # 数据块偏移量，用于在片段中定位数据块
        self.block_offset = block_offset
        # 数据块长度
        self.block_length = block_length

    def to_bytes(self):
        return pack(">IBIII",
                    self.payload_length,
                    self.message_id,
                    self.piece_index,
                    self.block_offset,
                    self.block_length)

    @classmethod
    def from_bytes(cls, payload):
        payload_length, message_id, piece_index, block_offset, block_length = unpack(">IBIII",
                                                                                     payload[:cls.total_length])
        if message_id != cls.message_id:
            raise WrongMessageException("Not a Request message")

        return Request(piece_index, block_offset, block_length)

# 用于传输具体的数据块内容
class Piece(Message):
    """
        PIECE = <length><message id><piece index><block offset><block>
        - length = 9 + block length (4 bytes)
        - message id = 7 (1 byte)
        - piece index =  zero based piece index (4 bytes)
        - block offset = zero based of the requested block (4 bytes)
        - block = block as a bytestring or bytearray (block_length bytes)
    """
    message_id = 7

    payload_length = -1
    total_length = -1

    def __init__(self, block_length, piece_index, block_offset, block):
        super(Piece, self).__init__()
        # 数据块长度
        self.block_length = block_length
        # 片段号
        self.piece_index = piece_index
        # 数据块偏移量
        self.block_offset = block_offset
        # 数据块的实际数据
        self.block = block

        self.payload_length = 9 + block_length
        self.total_length = 4 + self.payload_length

    def to_bytes(self):
        return pack(">IBII{}s".format(self.block_length),
                    self.payload_length,
                    self.message_id,
                    self.piece_index,
                    self.block_offset,
                    self.block)

    @classmethod
    def from_bytes(cls, payload):
        block_length = len(payload) - 13
        payload_length, message_id, piece_index, block_offset, block = unpack(">IBII{}s".format(block_length),
                                                                              payload[:13 + block_length])

        if message_id != cls.message_id:
            raise WrongMessageException("Not a Piece message")

        return Piece(block_length, piece_index, block_offset, block)

# 用于取消之前发出的数据块请求
class Cancel(Message):
    """CANCEL = <length><message id><piece index><block offset><block length>
        - length = 13 (4 bytes)
        - message id = 8 (1 byte)
        - piece index = zero based piece index (4 bytes)
        - block offset = zero based of the requested block (4 bytes)
        - block length = length of the requested block (4 bytes)"""
    message_id = 8

    payload_length = 13
    total_length = 4 + payload_length

    def __init__(self, piece_index, block_offset, block_length):
        super(Cancel, self).__init__()

        self.piece_index = piece_index
        self.block_offset = block_offset
        self.block_length = block_length

    def to_bytes(self):
        return pack(">IBIII",
                    self.payload_length,
                    self.message_id,
                    self.piece_index,
                    self.block_offset,
                    self.block_length)

    @classmethod
    def from_bytes(cls, payload):
        payload_length, message_id, piece_index, block_offset, block_length = unpack(">IBIII",
                                                                                     payload[:cls.total_length])
        if message_id != cls.message_id:
            raise WrongMessageException("Not a Cancel message")

        return Cancel(piece_index, block_offset, block_length)

# 用于告知对等方自己的监听端口号
class Port(Message):
    """
        PORT = <length><message id><port number>
            - length = 5 (4 bytes)
            - message id = 9 (1 byte)
            - port number = listen_port (4 bytes)
    """
    message_id = 9

    payload_length = 5
    total_length = 4 + payload_length

    def __init__(self, listen_port):
        super(Port, self).__init__()

        self.listen_port = listen_port

    def to_bytes(self):
        return pack(">IBI",
                    self.payload_length,
                    self.message_id,
                    self.listen_port)

    @classmethod
    def from_bytes(cls, payload):
        payload_length, message_id, listen_port = unpack(">IBI", payload[:cls.total_length])

        if message_id != cls.message_id:
            raise WrongMessageException("Not a Port message")

        return Port(listen_port)
