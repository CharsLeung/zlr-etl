# encoding: utf-8

"""
project = zlr(20200403备份)
file_name = ftp
author = Administrator
datetime = 2020/4/26 0026 上午 10:29
from = office desktop
"""
from ftplib import FTP


class FtpReader:

    def __init__(self, ip, port, user, pwd, bufferSize=1024, **kwargs):
        self.ftp = FTP()
        self.ftp.set_debuglevel(2)
        self.ftp.connect(ip, port)
        self.ftp.login(user, pwd)
        self.bufferSize = bufferSize
        print(self.ftp.getwelcome())
        pass

    def files(self):
        fs = []
        dirs = self.ftp.dir()
        print(12)
        for d in dirs:
            self.ftp.cwd(d)
            fs += self.ftp.nlst()
            pass


if __name__ == "__main__":
    FtpReader('125.83.85.173', 21, 'Chubby', 'j3PxqH2tvK0J-ftp').files()