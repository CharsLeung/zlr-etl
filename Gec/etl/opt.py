# encoding: utf-8

"""
project = zlr数据处理
file_name = opt
author = Administrator
datetime = 2020/4/29 0029 上午 9:38
from = office desktop
"""
import re, time

from Calf.data import BaseModel
from Gec.etl.utils import progress_bar
from Gec import workspace
from Gec.etl.core import Qcc


class Operating(Qcc):

    def __init__(self, ReturnString=None, **kwargs):
        Qcc.__init__(self, ReturnString)
        if len(self.source_patterns) == 0:
            self.source_patterns = self.load_regular_expression(
                workspace + '【数宜信】企业信息数据-属性字段一览表2.0.xlsx',
                '{}（标准结构）'.format('经营状况'))
        pass

    @staticmethod
    def run():
        bm = BaseModel(tn='qcc_original')
        bm2 = BaseModel(tn='qcc_format_jyzk')

        metaModel = '经营状况'

        enterprises = bm.query(
            sql={'metaModel': metaModel,
                 # 'name': '重庆鸿盾科技有限公司'
                 },
            # field={'content': 1, '_id': 0},
            no_cursor_timeout=True)
        i = 0
        etp = Operating()
        new = []
        start = time.time()
        count = enterprises.count()
        enterprises = etp.transfer_from_cursor(enterprises, False)
        for e in enterprises:
            # if i > 1001:
            #     break
            if e is not None:
                new.append(e)
            if len(new) > 100:
                # bm2.insert_batch(new)
                new.clear()
                progress_bar(
                    count, i, 'transfer qcc data and spend {} '
                              'seconds'.format(int(time.time() - start)))
            i += 1
            pass
        if len(new):
            # bm2.insert_batch(new)
            new.clear()
            progress_bar(
                count, i, 'transfer qcc data and spend {} '
                          'seconds'.format(int(time.time() - start)))
        if len(etp.logs):
            etp.save_logs('{}.csv'.format(metaModel))
        pass


Operating.run()
