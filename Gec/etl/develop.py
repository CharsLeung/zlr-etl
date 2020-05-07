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

# Develop.run()