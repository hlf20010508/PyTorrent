import ipaddress
import struct
import peer
from message import UdpTrackerConnection, UdpTrackerAnnounce, UdpTrackerAnnounceOutput
from peers_manager import PeersManager

__author__ = 'alexisgallepe'

import requests
import logging
from bcoding import bdecode
import socket
from urllib.parse import urlparse

# 尝试连接对等方的最大数量
MAX_PEERS_TRY_CONNECT = 30
# 最终成功连接的对等方的最大数量
MAX_PEERS_CONNECTED = 8


class SockAddr:
    def __init__(self, ip, port, allowed=True):
        self.ip = ip
        self.port = port
        # 可能用于筛选对等方，标记对等方是否允许连接
        self.allowed = allowed

    def __hash__(self):
        return "%s:%d" % (self.ip, self.port)


class Tracker(object):
    def __init__(self, torrent):
        self.torrent = torrent
        self.threads_list = []
        self.connected_peers = {}
        self.dict_sock_addr = {}
    # 从trackers服务器获取对等方
    def get_peers_from_trackers(self):
        # 遍历trackers服务器列表
        for i, tracker in enumerate(self.torrent.announce_list):
            # 当记录的对等方数量大于等于最大数量时就不再寻找对等方
            if len(self.dict_sock_addr) >= MAX_PEERS_TRY_CONNECT:
                break
            # 取出一个主链接（我不太懂为什么是这样，按理来说tracker就是一个字符串了，不应该是列表呀
            tracker_url = tracker[0]
            # 如果链接为http或https协议
            if str.startswith(tracker_url, "http"):
                try:
                    # 使用http爬虫获取对等方
                    self.http_scraper(self.torrent, tracker_url)
                except Exception as e:
                    logging.error("HTTP scraping failed: %s " % e.__str__())
            # 如果链接为udp协议
            elif str.startswith(tracker_url, "udp"):
                try:
                    # 使用udp爬虫获取对等方
                    self.udp_scrapper(tracker_url)
                except Exception as e:
                    logging.error("UDP scraping failed: %s " % e.__str__())

            else:
                logging.error("unknown scheme for: %s " % tracker_url)
        # 尝试链接对等方
        self.try_peer_connect()
        # 返回成功连接的对等方
        return self.connected_peers

    def try_peer_connect(self):
        logging.info("Trying to connect to %d peer(s)" % len(self.dict_sock_addr))
        # 遍历记录的所有对等方的地址信息
        # 第一个_记录的是字典的关键字，没有必要使用
        for _, sock_addr in self.dict_sock_addr.items():
            # 如果当前已连接的对等方数量超过最大数量就退出
            if len(self.connected_peers) >= MAX_PEERS_CONNECTED:
                break
            # 创建对等方实例
            new_peer = peer.Peer(int(self.torrent.number_of_pieces), sock_addr.ip, sock_addr.port)
            # 尝试与对等方进行连接，如果连接失败就跳过
            if not new_peer.connect():
                continue
            # BUG: 此行代码应该往下移到记录对等方后面，不然还没有更新
            print('Connected to %d/%d peers' % (len(self.connected_peers), MAX_PEERS_CONNECTED))
            # 记录成功连接的对等方，用对等方的IP地址:端口号作为关键字
            self.connected_peers[new_peer.__hash__()] = new_peer

    def http_scraper(self, torrent, tracker):
        # 定义发送到tracker的参数
        params = {
            'info_hash': torrent.info_hash, # 种子的哈希值
            'peer_id': torrent.peer_id, # 本客户端的peer id
            'uploaded': 0, # 已上传的字节数，初始为0
            'downloaded': 0, # 已下载的字节数，初始为0
            'port': 6881, # 本客户端监听的端口号，用于与对等方通信
            'left': torrent.total_length, # 还需要下载的字节数，初始为种子总大小
            'event': 'started' # 表明初始状态，刚刚交互，要开始下载。常见的值有started、stopped和completed
        }

        try:
            # 向tracker发送请求，获取对等方的信息
            answer_tracker = requests.get(tracker, params=params, timeout=5)
            # 从响应中解码出对等方列表
            list_peers = bdecode(answer_tracker.content)
            # 用作从压缩字符串提取信息的光标
            offset=0
            # 检查list_peers['peers']是否为列表，若不是列表，它可能是一个压缩的字符串，需要解压缩
            if not type(list_peers['peers']) == list:
                '''
                    - Handles bytes form of list of peers
                    - IP address in bytes form:
                        - Size of each IP: 6 bytes
                        - The first 4 bytes are for IP address
                        - Next 2 bytes are for port number
                    - To unpack initial 4 bytes !i (big-endian, 4 bytes) is used.
                    - To unpack next 2 byets !H(big-endian, 2 bytes) is used.
                '''
                # 每个对等方的信息占6个字节，4字节IP+2字节端口号
                for _ in range(len(list_peers['peers'])//6):
                    # 提取4字节的IP地址，!i表示大端格式的整数
                    ip = struct.unpack_from("!i", list_peers['peers'], offset)[0]
                    # 将提取的IP地址转换为可读的字符串格式
                    ip = socket.inet_ntoa(struct.pack("!i", ip))
                    # 光标后移4个字节，开始提取端口号
                    offset += 4
                    # "!H" 表示大端格式的短整型
                    # BUG: 此处port需要用int转换
                    port = struct.unpack_from("!H",list_peers['peers'], offset)[0]
                    # 提取完端口号后，后移2个字节，准备提取下一个对等方信息
                    offset += 2
                    # 创建实例存储对等方的IP地址和端口号信息
                    s = SockAddr(ip,port)
                    # 用IP地址:端口号作为唯一关键字存入字典
                    self.dict_sock_addr[s.__hash__()] = s
            # 如果list_peers['peers']为列表，则信息没有被压缩，直接迭代提取信息
            else:
                for p in list_peers['peers']:
                    # BUG: 此处port需要用int转换
                    s = SockAddr(p['ip'], p['port'])
                    self.dict_sock_addr[s.__hash__()] = s

        except Exception as e:
            logging.exception("HTTP scraping failed: %s" % e.__str__())

    def udp_scrapper(self, announce):
        torrent = self.torrent
        # 从tracker链接里取出主机名和端口号的信息
        # BUG: 根据python版本的不同，较老的版本无法使用urllib.parse.urlparse解析
        # 而且会影响requests.utils.urlparse解析
        # 可以使用如下方式解析
        # parsed = urlparse(announce)
        # try:
        #     ip, port = socket.gethostbyname(parsed.hostname), parsed.port
        # except:
        #     hostname = ':'.join(parsed.netloc.split(':')[:-1]).lstrip('[').rstrip(']')
        #     port = int(parsed.netloc.split(':')[-1])
        #     ip = socket.gethostbyname(hostname)
        parsed = urlparse(announce)
        # 创建一个用于 UDP 通信的套接字
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # 设置套接字选项以允许地址重用
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.settimeout(4)
        # 解析出IP地址和端口号
        ip, port = socket.gethostbyname(parsed.hostname), parsed.port
        # 如果地址为私有，则返回
        if ipaddress.ip_address(ip).is_private:
            return
        # 向tracker发送连接请求
        tracker_connection_input = UdpTrackerConnection()
        # 发送请求，等待响应
        response = self.send_message((ip, port), sock, tracker_connection_input)

        if not response:
            raise Exception("No response for UdpTrackerConnection")
        # 创建实例用于解析响应
        tracker_connection_output = UdpTrackerConnection()
        # 用响应填充实例
        tracker_connection_output.from_bytes(response)
        # 创建实例，准备发送请求，参数包含种子哈希值，从响应处获得的连接ID，以及本客户端的peer id
        # 监听的端口号被写在UdpTrackerAnnounce.to_bytes的port里，为8000
        tracker_announce_input = UdpTrackerAnnounce(torrent.info_hash, tracker_connection_output.conn_id,
                                                    torrent.peer_id)
        # 发送请求，等待响应
        response = self.send_message((ip, port), sock, tracker_announce_input)

        if not response:
            raise Exception("No response for UdpTrackerAnnounce")
        # 创建实例用于解析响应
        tracker_announce_output = UdpTrackerAnnounceOutput()
        # 填充
        tracker_announce_output.from_bytes(response)
        # 遍历解析出的对等方地址列表
        for ip, port in tracker_announce_output.list_sock_addr:
            sock_addr = SockAddr(ip, port)
            # 只增加新的对等方
            if sock_addr.__hash__() not in self.dict_sock_addr:
                self.dict_sock_addr[sock_addr.__hash__()] = sock_addr

        print("Got %d peers" % len(self.dict_sock_addr))

    def send_message(self, conn, sock, tracker_message):
        # 将消息对象转换为字节序列
        message = tracker_message.to_bytes()
        # 获取事务ID，用于后续验证响应
        trans_id = tracker_message.trans_id
        # 获取动作类型（如连接、宣告等），也用于验证响应
        action = tracker_message.action
        # 计算消息的长度
        size = len(message)
        # 发送消息
        sock.sendto(message, conn)

        try:
            # 从套接字读取tracker的响应
            response = PeersManager._read_from_socket(sock)
        except socket.timeout as e:
            # tracker超时
            logging.debug("Timeout : %s" % e)
            return
        except Exception as e:
            logging.exception("Unexpected error when sending message : %s" % e.__str__())
            return
        # 收到的消息长度不足，信息缺失
        if len(response) < size:
            logging.debug("Did not get full message.")
        # 验证响应中的动作类型和事务ID是否与发送的消息匹配。这是为了确保响应确实是对发送的特定消息的回应。
        if action != response[0:4] or trans_id != response[4:8]:
            logging.debug("Transaction or Action ID did not match")

        return response
