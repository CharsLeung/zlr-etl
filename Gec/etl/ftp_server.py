# encoding: utf-8

"""
project = zlr数据处理
file_name = ftp_server
author = Administrator
datetime = 2020/4/26 0026 下午 14:36
from = office desktop
"""
import logging

from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler, ThrottledDTPHandler
from pyftpdlib.servers import FTPServer
from pyftpdlib.log import LogFormatter

# 记录日志，默认情况下日志仅输出到屏幕（终端）
logger = logging.getLogger()
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
fh = logging.FileHandler(filename='ftp_server.log')
ch.setFormatter(LogFormatter())
fh.setFormatter(LogFormatter())
logger.addHandler(ch)  # 将日志输出至屏幕
logger.addHandler(fh)  # 将日志输出至文件

# 实例化虚拟用户，这是FTP验证首要条件
authorizer = DummyAuthorizer()
# 添加用户权限和路径，括号内的参数是(用户名， 密码， 用户目录， 权限),可以为不同的用户添加不同的目录和权限
authorizer.add_user("Chubby", "j3PxqH2tvK0J-ftp", "D:\graph_data\基本信息", perm="elradfmw")
# 添加匿名用户 只需要路径
# authorizer.add_anonymous("D:\graph_data\基本信息")

# 初始化ftp句柄
handler = FTPHandler
handler.authorizer = authorizer

# 添加被动端口范围
handler.passive_ports = range(2000, 2333)

# 下载上传速度设置
dtp_handler = ThrottledDTPHandler
dtp_handler.read_limit = 1024 * 1024  # 300kb/s
dtp_handler.write_limit = 1024 * 1024  # 300kb/s

# 监听ip 和 端口,linux里需要root用户才能使用21端口
server = FTPServer(("0.0.0.0", 21), handler)

# 最大连接数
server.max_cons = 20
server.max_cons_per_ip = 15

# 开始服务，自带日志打印信息
server.serve_forever()
