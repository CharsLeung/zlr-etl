# encoding: utf-8

"""
project = zlr数据处理
file_name = risk
author = Administrator
datetime = 2020/4/29 0029 下午 16:55
from = office desktop
"""
import re, time

from Calf.data import BaseModel
from Gec import workspace
from Gec.etl.core import Qcc
from Gec.etl.utils import progress_bar


class Risk(Qcc):

    def __init__(self, ReturnString=None, **kwargs):
        Qcc.__init__(self, ReturnString)
        if len(self.source_patterns) == 0:
            self.source_patterns = self.load_regular_expression(
                workspace + '【数宜信】企业信息数据-属性字段一览表2.0.xlsx',
                '{}（标准结构）'.format('经营风险'))
        pass

# Risk.run()