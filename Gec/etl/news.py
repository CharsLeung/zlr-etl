# encoding: utf-8

"""
project = zlr数据处理
file_name = news
author = Administrator
datetime = 2020/5/1 0001 下午 22:08
from = office desktop
"""
import time

from Gec import workspace
from Gec.etl.core import Qcc
from Gec.etl.utils import progress_bar


class News(Qcc):

    def __init__(self, ReturnString=None, **kwargs):
        Qcc.__init__(self, ReturnString)
        if len(self.source_patterns) == 0:
            self.source_patterns = self.load_regular_expression(
                workspace + '【数宜信】企业信息数据-属性字段一览表2.0.xlsx',
                '{}（标准结构）'.format('公司新闻'))
        pass


# News.run()