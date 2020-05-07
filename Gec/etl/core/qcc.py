# encoding: utf-8

"""
project = zlr数据处理
file_name = base
author = Administrator
datetime = 2020/4/26 0026 上午 11:23
from = office desktop
"""
import re
import time
import pandas as pd
import datetime as dt

from Gec.exception import SuccessMessage, \
    WarningMessage, ErrorMessage, ExceptionInfo
# from Gec.etl.core import Content
from Gec import project_dir
from Gec.etl.core import ContentTree
from Gec.etl.utils import progress_bar


class Qcc(ContentTree):

    def __init__(self, ReturnString=None):
        ContentTree.__init__(self, None)
        self.__load_content__(ReturnString)
        pass

    def __load_content__(self, ReturnString):
        """
        :param ReturnString:
        :return:
        """
        if ReturnString is not None:
            if isinstance(ReturnString, dict):
                pass
            else:
                try:
                    ReturnString = eval(ReturnString)
                except Exception:
                    print(ReturnString)
                    raise TypeError('not json object')
            ks = ReturnString.keys()
            self.name = ReturnString['name'] if 'name' in ks else None
            self.metaModel = ReturnString['metaModel'] if 'metaModel' in ks else None
            self.url = ReturnString['url'] if 'url' in ks else None
            self.headers = ReturnString['headers'] if 'headers' in ks else None
            self.get = ReturnString['get'] if 'get' in ks else None
            self.update_date = ReturnString['date'] if 'date' in ks else None
            cnt = ReturnString['content'] if 'content' in ks else None
            self.doc_from = ReturnString['_id'] if '_id' in ks else None
            super().__load_content__(cnt)
        pass

    def reload_content(self, ReturnString):
        self.__load_content__(ReturnString)
        pass

    def to_dict(self):
        return dict(
            name=self.name,
            metaModel=self.metaModel,
            url=self.url,
            headers=self.headers,
            get=self.get,
            date=self.update_date,
            hash=hash(str(self.format_content)),
            doc_from=self.doc_from,
            **self.format_content
        )

    @classmethod
    def load_regular_expression(cls, path, sheet):
        """
        从excel文档中加载标准化信息
        :return:
        """
        # TODO(leung): 保持标准化模板的及时性
        regs = pd.read_excel(
            project_dir + '\【数宜信】企业信息数据-属性字段一览表2.0.xlsx',
            sheet_name=sheet,
            header=[1])
        regs['匹配模式'] = regs['匹配模式'].map(
            lambda x: re.sub('\d+', lambda _: '\d+', x)
        )
        regs['匹配模式-修正'].fillna(regs['匹配模式'], inplace=True)
        regs = regs.loc[:, ['完整目录', '匹配模式-修正', '值匹配模式',
                            '值类型', '缺省填充']]
        # regs.sort_values(['完整目录'], ascending=False, inplace=True)
        regs['匹配模式-修正'] = regs['匹配模式-修正'].map(
            lambda x: None if pd.isnull(x) else x
        )
        regs['值匹配模式'] = regs['值匹配模式'].map(
            lambda x: None if pd.isnull(x) else x
        )
        regs['值类型'].fillna('str', inplace=True)
        regs['缺省填充'] = regs['缺省填充'].map(
            lambda x: None if pd.isnull(x) else x
        )
        rs = {}
        for i, r in regs.iterrows():
            # 编译所有的匹配模式
            if r['匹配模式-修正'] is not None and 'func:' != r['匹配模式-修正'][0:5]:
                a = r['匹配模式-修正']
            else:
                a = r['匹配模式-修正']
            if r['值匹配模式'] is not None and 'func:' != r['值匹配模式'][0:5]:
                b = r['值匹配模式']
            else:
                b = r['值匹配模式']
            rs[r['完整目录']] = [a, b, r['值类型'], r['缺省填充']]
        print(SuccessMessage('success load standardization data '
                             'doc({}).'.format(sheet)))
        return rs

    def transfer(self, print_process=False):
        try:
            self.replace_keys(print_process=print_process)
            self.replace_values(print_process=print_process)
            self.format_dim_2_content_to_dict()
            d = self.to_dict()
        except Exception as e:
            self.to_logs(str(e), 'EXCEPTION', self.name)
            d = None
        return d
        pass

    def transfer_from_cursor(self, cursor, print_process=False):
        """
        通过异步的方式生成一个数据转换的Generator
        :param cursor: 一个数据库游标，或一个Generator
        :param print_process:
        :return:
        """
        for _ in cursor:
            try:
                self.reload_content(_)
                self.replace_keys(print_process=print_process)
                self.replace_values(print_process=print_process)
                self.format_dim_2_content_to_dict()
                d = self.to_dict()
                # d = self.to_tree()
            except Exception as e:
                self.to_logs(str(e), 'EXCEPTION', _['name'])
                d = None
            yield d
        pass

    def run(self, cursor, driver, insert_batch_size=10):
        i = 0
        new = []
        count = cursor.count()
        start = time.time()
        enterprises = self.transfer_from_cursor(cursor, False)
        for e in enterprises:
            # if i > 1001:
            #     break
            if e is not None:
                new.append(e)
            if len(new) > insert_batch_size:
                # driver.insert_batch(new)
                new.clear()
                progress_bar(
                    count, i, 'transfer qcc data and spend {} '
                              'seconds'.format(int(time.time() - start)))
            i += 1
            pass
        if len(new):
            # driver.insert_batch(new)
            new.clear()
            progress_bar(
                count, i, 'transfer qcc data and spend {} '
                          'seconds'.format(int(time.time() - start)))
        if len(self.logs):
            self.save_logs('{}.csv'.format(self.__class__))
        pass

    @staticmethod
    def textPhrase(text, pattern=None, **kwargs):
        """
        对一个文本短语进行清洗
        所谓文本短语，就是区别与句子和段落的短小词语
        所以不应该出现空格、\n \t等等
        :param pattern:
        :param text:
        :return:
        """
        if isinstance(text, str):
            return text.replace(' ', '') \
                .replace('\n', '') \
                .replace('\t', '')
            pass
        else:
            return str(text)

    @staticmethod
    def getAmount(text, pattern=None, **kwargs):
        """
        从带有金额、价格、数量单位的短语中获取数额
        支持会计格式的数字格式
        :param text:
        :param pattern:
        :param kwargs:
        :return:
        """
        _ = Qcc.textPhrase(text)
        # (\d*(,|，)*\d*)*(\.){0,1}\d*
        _ = re.search('(\d*(,|，)*\d*)*(\.){0,1}\d*', _)
        dw = _.group(0) if _ is not None else None
        return dw

    @staticmethod
    def getAmountUnit(text, pattern=None, **kwargs):
        """
        从带有金额、价格、数量单位的短语中获取单位
        :param text:
        :return:
        """
        _ = Qcc.textPhrase(text)
        # 数量单位是紧跟在数后面的
        # TODO: 10(万元)中的单位还匹配不出来
        _ = re.search('(?<=\d)[\u4e00-\u9fa5]+', _)
        dw = _.group(0) if _ is not None else None
        return dw

    def getAmountUnitFromKey(self, value, pattern, **kwargs):
        """
        有些金额、数量的单位存在key当中的，例如“认缴出资(万元):200”，
        而不是“认缴出资:200万元”
        :param value:
        :param pattern:
        :return:
        """
        # 1.从key里面提取金额单位
        org = self.getOriginalFromMatch(kwargs['standard_key'])
        _ = re.search('(?<=\(|（|_)[\u4e00-\u9fa5]*', org)
        dw = _.group(0) if _ is not None else None
        return dw
        pass

    @staticmethod
    def getTextFromHyperlinksText(value, pattern=None, **kwargs):
        """
        从带有超链接的文本中提取出文本信息，文本与连接中间
        有一个“|”分割，这是前面约定好的。
        例如从“张三|pl_12323445465757”中提取出“张三”
        :param value:
        :param pattern:
        :param kwargs:
        :return:
        """
        return value.split('|')[0]

    @staticmethod
    def getUrlFromHyperlinksText(value, pattern=None, **kwargs):
        """
        从带有超链接的文本中提取出连接信息，文本与连接中间
        有一个“|”分割，这是前面约定好的。
        从“张三|pl_12323445465757”中提取出“pl_12323445465757”
        :param value:
        :param pattern:
        :param kwargs:
        :return:
        """
        _ = value.split('|')
        return _[1] if len(_) > 1 else None

    @staticmethod
    def getDate(value, pattern=None, **kwargs):
        """
        提取日期格式字符串，不含时间
        :param value:
        :param pattern:
        :param kwargs:
        :return:
        """
        _ = Qcc.textPhrase(value)
        _ = re.search('^\d{4}(-|/|.|年)\d{1,2}(-|/|.|月)\d{1,2}', _)
        dw = _.group(0) if _ is not None else None
        if dw is not None:
            dw = dw.replace('/', '-').replace('.', '-').\
                replace('年', '-').replace('月', '-').\
                replace('日', '')
        return dw

    @staticmethod
    def getDateRange(value, pattern=None, **kwargs):
        """
        提取由两个日期组成的日期区间
        :param value:
        :param pattern:
        :param kwargs:
        :return:
        """
        _ = Qcc.textPhrase(value)
        # _ = re.findall('\d{4}(-|/|.|年)\d{1,2}(-|/|.|月)\d{1,2}', _)
        # if _ is not None:
        #     dr = []
        #     for d in _.group():
        #         d = d.replace('/', '-').replace('.', '-'). \
        #             replace('年', '-').replace('月', '-'). \
        #             replace('日', '')
        #         dr.append(d)
        #         pass
        #     return '至'.join(dr)
        # else:
        #     return None
        return _

    @staticmethod
    def getPercent(value, pattern=None, **kwargs):
        """
        提取百分比
        :param value:
        :param pattern:
        :param kwargs:
        :return:
        """
        _ = Qcc.textPhrase(value)
        _ = re.search('\d+(\.)*\d*%', _)
        dw = _.group(0) if _ is not None else None
        return dw

    @staticmethod
    def getTelephone(value, pattern=None, **kwargs):
        """
        提取电话号码
        :param value:
        :param pattern:
        :param kwargs:
        :return:
        """
        _ = Qcc.textPhrase(value)
        _ = re.search('(\d{3}-\d{8}|\d{4}-\{7,8})|(\d{11})', _)
        dw = _.group(0) if _ is not None else None
        return dw

    @staticmethod
    def getEmail(value, pattern=None, **kwargs):
        """
        提取邮箱
        :param value:
        :param pattern:
        :param kwargs:
        :return:
        """
        _ = Qcc.textPhrase(value)
        _ = re.search("[\w!#$%&'*+/=?^_`{|}~-]+(?:\."
                      "[\w!#$%&'*+/=?^_`{|}~-]+)*@"
                      "(?:[\w](?:[\w-]*[\w])?\.)+"
                      "[\w](?:[\w-]*[\w])?", _)
        dw = _.group(0) if _ is not None else None
        return dw

    @staticmethod
    def getWebsite(value, pattern=None, **kwargs):
        """
        提取网站
        :param value:[a-zA-z]+://[^\s]*
        :param pattern:
        :param kwargs:
        :return:
        """
        _ = Qcc.textPhrase(value)
        _ = re.search("[a-zA-z]+://[^\s]*", _)
        dw = _.group(0) if _ is not None else None
        return dw
