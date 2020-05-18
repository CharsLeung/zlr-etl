# encoding: utf-8

"""
project = zlr(20200403备份)
file_name = ftp
author = Administrator
datetime = 2020/4/26 0026 上午 10:29
from = office desktop
"""
import sys
import time
import win32api
import win32event
import win32service
import win32serviceutil
import servicemanager

from Calf.net.utils import get_export_ip
from Calf.ftp import FtpClient
from Calf.pywinservice import PyService
from Calf.dev import ModelRun
from urllib.request import urlopen


# def get_export_ip():
#     ip = eval(str(urlopen('http://httpbin.org/ip').read(),
#                   'utf-8'))['origin']
#     return ip


class SynExportIp:

    @classmethod
    def is_trade_day(cls, day):
        return True

    @classmethod
    def execute(cls, **kwargs):
        # print(kwargs)
        ip = get_export_ip()
        fc = FtpClient(
            '123.234.5.140',
            21,
            'Chubby',
            'j3PxqH2tvK0J-ftp'
        )
        fc.push('this_pc_ip.txt', '{}.txt'.format(ip))
        pass


class SynExportIpService(win32serviceutil.ServiceFramework):

    _svc_name_ = 'SynExportIpService'
    _svc_display_name_ = 'SynExportIpService'
    _svc_description_ = '同步本机的出口ip到140上'

    def __init__(self, args):
        self.log('init')
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.stop_event = win32event.CreateEvent(None, 0, 0, None)

    def SvcDoRun(self):
        self.ReportServiceStatus(win32service.SERVICE_START_PENDING)
        try:
            self.ReportServiceStatus(win32service.SERVICE_RUNNING)
            self.log('start')
            self.start()
            self.log('wait')
            win32event.WaitForSingleObject(self.stop_event, win32event.INFINITE)
            self.log('done')
        except BaseException as e:
            self.log('Exception : %s' % e)
            self.SvcStop()

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        self.log('stopping')
        self.stop()
        self.log('stopped')
        win32event.SetEvent(self.stop_event)
        self.ReportServiceStatus(win32service.SERVICE_STOPPED)

    def stop(self):
        pass

    def log(self, msg):
        servicemanager.LogInfoMsg(str(msg))

    def start(self):
        ModelRun.DScheduler(
            SynExportIp,
            execute_date='00:01:00-23:56:00',
            execute_interval=5 * 60,
            # info='Synchronize ip success.'
        )
        pass

    @staticmethod
    def runService():
        if len(sys.argv) == 1:
            servicemanager.Initialize()
            servicemanager.PrepareToHostSingle(SynExportIpService)
            servicemanager.StartServiceCtrlDispatcher()
        else:
            win32serviceutil.HandleCommandLine(SynExportIpService)
        pass


if __name__ == "__main__":
    SynExportIpService.runService()
