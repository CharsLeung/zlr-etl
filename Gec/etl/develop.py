# encoding: utf-8

"""
project = zlr数据处理
file_name = develop
author = Administrator
datetime = 2020/4/30 0030 下午 15:23
from = office desktop
"""
import time

from Gec import workspace
from Gec.etl.core import Qcc
from Gec.etl.utils import progress_bar


class Develop(Qcc):

    def __init__(self, ReturnString=None, **kwargs):
        Qcc.__init__(self, ReturnString)
        if len(self.source_patterns) == 0:
            self.source_patterns = self.load_regular_expression(
                workspace + '【数宜信】企业信息数据-属性字段一览表2.0.xlsx',
                '{}（标准结构）'.format('企业发展'))
        pass

    @staticmethod
    def run(enterprises, driver):
        # bm2 = BaseModel(tn='qcc_format_jbxx')
        i = 0
        etp = Develop()
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
                driver.insert_batch(new)
                new.clear()
                progress_bar(
                    count, i, 'transfer qcc data and spend {} '
                              'seconds'.format(int(time.time() - start)))
            i += 1
            pass
        if len(new):
            driver.insert_batch(new)
            new.clear()
            progress_bar(
                count, i, 'transfer qcc data and spend {} '
                          'seconds'.format(int(time.time() - start)))
        if len(etp.logs):
            etp.save_logs('{}.csv'.format('企业发展'))
        pass


# Develop.run()