# encoding: utf-8

"""
project = zlr数据处理
file_name = opt
author = Administrator
datetime = 2020/4/29 0029 上午 9:38
from = office desktop
"""
import re, time

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


# Operating.run()
