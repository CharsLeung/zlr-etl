# encoding: utf-8

"""
project = zlr数据处理
file_name = base
author = Administrator
datetime = 2020/4/26 0026 下午 16:33
from = office desktop
"""
import re, time

from Gec import workspace
from Gec.etl.core import Qcc
from Gec.etl.utils import progress_bar


class Enterprise(Qcc):

    def __init__(self, ReturnString=None, **kwargs):
        Qcc.__init__(self, ReturnString)
        if len(self.source_patterns) == 0:
            self.source_patterns = self.load_regular_expression(
                workspace + '【数宜信】企业信息数据-属性字段一览表2.0.xlsx',
                '{}（标准结构）'.format('基本信息'))
        pass

    def holderSubscription(self, value, pattern, **kwargs):
        """
        处理股东信息当中，认缴出资、实缴出资这两个字段，
        原因是原字段是“认缴出资(万元)”括号里面可能还是
        万美元等等，弄这个函数的目的是要把原字段中的这
        个货币单位搞出来
        :param value:
        :param pattern:
        :return:
        """
        # 1.先把金额提取出来

        # 2.从key里面提取金额单位
        org = self.getOriginalFromMatch(kwargs['standard_key'])
        _ = re.search('(?<=\(|（|_)[\u4e00-\u9fa5]*', org)
        dw = _.group(0) if _ is not None else None
        return dw
        pass

    def getNameFromCell(self, value, pattern, **kwargs):
        """
        提取一个人名，对于一个人来说，他可能带有标签、链接，
        例如 “张三 失信被执行人 限制高消费|pl_123453543”
        这种格式，现在只需要提取出“张三”即可，
        使用getTextFromHyperlinksText函数，会提出一大串
        :param value:
        :param pattern:
        :param kwargs:
        :return:
        """
        _ = self.getTextFromHyperlinksText(
            value, pattern, **kwargs)
        _ = _.split(' ')[0]
        return self.textPhrase(_)

    def getAddressUrlFromCell(self, value, pattern, **kwargs):
        """
        qcc基本信息里面工商信息的企业地址，带有附近企业这个链接
        :param value:
        :param pattern:
        :param kwargs:
        :return:
        """
        return self.getUrlFromHyperlinksText(
            value, pattern, **kwargs
        )

    def getAddressFromCell(self, value, pattern, **kwargs):
        _ = self.getTextFromHyperlinksText(
            value, pattern, **kwargs)
        _ = _.split(' ')[0]
        return self.textPhrase(_)
        pass

    @staticmethod
    def run(enterprises, driver):
        # bm2 = BaseModel(tn='qcc_format_jbxx')
        i = 0
        etp = Enterprise()
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
            etp.save_logs('{}.csv'.format('基本信息'))
        pass


# Enterprise.run()