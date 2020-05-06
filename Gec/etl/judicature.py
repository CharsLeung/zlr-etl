# encoding: utf-8

"""
project = zlr数据处理
file_name = judicature
author = Administrator
datetime = 2020/5/1 0001 下午 21:32
from = office desktop
"""
import time

from Calf.data import BaseModel
from Gec import workspace
from Gec.etl.core import Qcc
from Gec.etl.utils import progress_bar


class Judicature(Qcc):

    def __init__(self, ReturnString=None, **kwargs):
        Qcc.__init__(self, ReturnString)
        if len(self.source_patterns) == 0:
            self.source_patterns = self.load_regular_expression(
                workspace + '【数宜信】企业信息数据-属性字段一览表2.0.xlsx',
                '{}（标准结构）'.format('法律诉讼'))
        pass

    @staticmethod
    def run():
        bm = BaseModel(tn='qcc_original')
        bm2 = BaseModel(tn='qcc_format_flss')

        metaModel = '法律诉讼'

        enterprises = bm.query(
            sql={'metaModel': metaModel,
                 # 'name': '重庆市江北区烽雨五金建材经营部'
                 },
            # field={'content': 1, '_id': 0},
            no_cursor_timeout=True)
        i = 0
        etp = Judicature()
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


Judicature.run()