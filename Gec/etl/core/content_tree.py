# encoding: utf-8

"""
project = zlr数据处理
file_name = cnt2
author = Administrator
datetime = 2020/4/30 0030 下午 18:08
from = office desktop
"""
import re
import pandas as pd
import datetime as dt

from Gec.exception import SuccessMessage, \
    WarningMessage, ErrorMessage, ExceptionInfo
from Gec.etl.core import JsonTree


class ContentTree(object):
    types = {
        'int': lambda x: int(x) if x is not None else x,
        'float': lambda x: float(x) if x is not None else x,
        'str': lambda x: str(x) if x is not None else x,
        'list': lambda x: eval(x)
    }

    match = {}

    # patterns属性是一个关键的数据，它代表了数据清洗的关键
    # 它应该是一个二维数组，每个元素有三个子元素，分别是：
    # 1.标准的字段域结构；2.针对字段域结构的reg；3.针对取值的reg
    source_patterns = {}

    def __init__(self, content=None):
        self.source_dim_2_content = []
        self.format_dim_2_content = []
        self.format_content = {}
        self.content = content
        self.patterns = self.source_patterns
        pass

    def __load_content__(self, content):
        """
        :param content:
        :return:
        """
        self.source_dim_2_content = []
        self.format_dim_2_content = []
        self.format_content = {}
        self.content = content
        self.patterns = self.source_patterns
        self.logs = []
        pass

    def to_logs(self, info, tp='LOG', name=''):
        self.logs.append({
            'datetime': dt.datetime.now(),
            'info': info,
            'type': tp,
            'name': name
        })
        pass

    def save_logs(self, path):
        logs = pd.DataFrame(self.logs)
        logs.to_csv(path, index=False)
        pass

    def getOriginalFromMatch(self, fmt):
        """
        通过标准后的字段找到原始字段
        :param fmt:
        :return:
        """
        for k, v in zip(self.match.keys(), self.match.values()):
            if fmt in v:
                return k
        return None

    def get_keys(self, _, root='', sep='-', return_value=False,
                 filter_key=[], keep_key=[], value_sep='\n'):
        """
        从一个结构层次比较复杂的dict中分析出key的结构层次，
        默认以'-'分割所属关系
        :param value_sep:
        :param keep_key:
        :param filter_key:
        :param return_value:
        :param sep:
        :param _:
        :param root:
        :return:
        """
        # category = list()
        if isinstance(_, list):
            if len(_):
                for sv in _:
                    self.get_keys(sv, root, sep, return_value,
                                  filter_key, keep_key, value_sep)
            else:
                self.source_dim_2_content.append([root, {}])
                pass
        elif isinstance(_, dict):
            for k, v in zip(_.keys(), _.values()):
                if k in filter_key:
                    continue
                if isinstance(v, dict):
                    self.get_keys(v, '{}{}{}'.format(root, sep, k),
                                  sep, return_value, filter_key,
                                  keep_key, value_sep)
                    pass
                elif isinstance(v, list):
                    if len(v):
                        if len(v) == 1:
                            sv = v[0]
                            if not isinstance(sv, dict):
                                _ = '{}{}{}'.format(root, sep, k)
                                self.source_dim_2_content.append([_, sv])
                            else:
                                # if '序号' not in sv.keys():
                                sv['序号'] = 1
                                self.get_keys(
                                    v[0], '{}{}{}'.format(
                                        root, sep, '{}-#1'.format(k)),
                                    sep, return_value, filter_key,
                                    keep_key, value_sep)
                        else:
                            xh = 1
                            if sum([isinstance(sv, (str, int, float)) for sv in v]) == len(v):
                                # 在原始数据当中存在取值为一个list，原始为简单类型
                                _ = '{}{}{}'.format(root, sep, k)
                                self.source_dim_2_content.append([_, v])
                            else:
                                for sv in v:
                                    if not isinstance(sv, dict):
                                        _ = '{}{}{}'.format(root, sep, k)
                                        self.source_dim_2_content.append([_, sv])
                                    else:
                                        # if '序号' not in sv.keys():
                                        sv['序号'] = xh
                                        xh += 1
                                        self.get_keys(
                                            sv, '{}{}{}'.format(
                                                root, sep, '{}-#{}'.format(k, sv['序号'])),
                                            sep, return_value, filter_key,
                                            keep_key, value_sep)
                            pass

                    else:
                        self.source_dim_2_content.append([
                            '{}{}{}'.format(root, sep, k), {}])
                        pass
                else:
                    if return_value:
                        self.source_dim_2_content.append([
                            '{}{}{}'.format(root, sep, k), v])
                    pass
        else:
            self.source_dim_2_content.append([root, _])
        # return category

    def get_source_dim_2_content(self):
        """
        把多层结构的json数据转换成二维平面的数据
        :return:
        """
        # self.content.pop('认证信息')
        # self.content.pop('工商信息')
        # self.content.pop('工商股东')
        # self.content.pop('对外投资')
        # 返回的路径链中的序号是根据list类型重新计算添加的，这样
        # 避免了原始数据把多个表格和到一起引起的序号重复了
        self.get_keys(self.content, root='content',
                      sep='-', return_value=True,
                      value_sep='<tbl>')
        ds = self.source_dim_2_content
        data = []
        for d in ds:
            # k, v = d.split(':')[0], ':'.join(d.split(':')[1:])
            if '序号' in d[0]:
                continue
            data.append(d)

        self.source_dim_2_content = data
        # return dim_2_data
        pass

    def replace_keys(self, print_process=False):
        """
        对所有key值标准化,无匹配的字段将会被忽略掉
        :return:
        """
        if self.source_dim_2_content is None or len(
                self.source_dim_2_content) == 0:
            self.get_source_dim_2_content()
        dim_2_data = []
        new_pattern = {}

        def trans_key(key, pattern, **kwargs):
            if pattern is None:
                f_k = key
            elif pattern[0:5] == 'func:':
                # 说明这是一个函数名，调用即可
                f_k = getattr(self, pattern[1][5:])(
                    key, pattern, **kwargs)
            else:  # 当做一个正则表达式
                _ = re.search(pattern, key)
                if _ is not None:
                    f_k = kwargs['standard_key']
                    # print(pattern, key)
                else:
                    f_k = None
            return f_k

        def replace_standard_key(s, f):
            if '#' in s:
                ss = s.split('-')
                idx = []
                for i in range(len(ss)):
                    if '#' in ss[i]:
                        idx.append([i, ss[i]])
                ff = f.split('-')
                for i in idx:
                    ff.insert(i[0], i[1])
                return '-'.join(ff)
            else:
                return f

        match_type = None
        for d in self.source_dim_2_content:
            ms = []
            if d[0] in self.match.keys():
                ms = self.match[d[0]]
                for fk in ms:
                    dim_2_data.append([fk, d[1]])
                    # fk是带有索引的标准key,去掉索引后一定在原pattern当中
                    p = self.source_patterns[re.sub('-#\d+', '', fk)]
                    new_pattern[fk] = p
                match_type = 'cache'
            else:
                for pk, pv in zip(self.source_patterns.keys(),
                                  self.source_patterns.values()):
                    # TODO:根据原始字段链的序号索引来创建带有索引的标准链
                    # 序号索引存在于两个字段名之间的，那么标准化后的序号索
                    # 引就依赖原始结构，这点可能需要改进
                    sk = replace_standard_key(d[0], pk)
                    fk = trans_key(d[0], pv[0], standard_key=sk)
                    if fk is not None:
                        dim_2_data.append([fk, d[1]])
                        new_pattern[fk] = pv
                        ms.append(fk)
                        match_type = 'regular'
                    pass
                if len(ms):
                    self.match[d[0]] = ms
                if len(ms) > 1 and print_process:
                    print(WarningMessage('multiple replace for {}: {}.'
                                         ''.format(d[0], ms)))
            if print_process:
                print(SuccessMessage('replace({}): {} => {}'.format(
                        match_type, d[0], ms)))
                pass
            if len(ms) == 0 and print_process:
                print(ErrorMessage('mismatch {}:{}'.format(
                    d[0], d[1])))
        self.format_dim_2_content = dim_2_data
        self.patterns = new_pattern
        pass

    def engine_for_data_clean(self, value, pattern, **kwargs):
        """
        值清洗中转引擎
        :param value:
        :param pattern:
        :return:
        """
        # TODO: 默认值还没有实现
        if pattern[1] is None:  # 无清洗规则
            f_v = value
        elif pattern[1][0:5] == 'func:':
            # 说明这是一个函数名，调用即可
            f_v = getattr(self, pattern[1][5:])(value, pattern, **kwargs)
        else:  # 当做一个正则表达式，无匹配返回None
            _ = re.search(pattern[1], value)
            if _ is not None:
                f_v = _.group(0)
            else:
                f_v = None

        if f_v is not None:
            if isinstance(f_v, str) and len(f_v) == 0:
                f_v = None  # 即将'' 替换成None
            else:
                try:
                    # 值类型转换
                    f_v = self.types[pattern[2]](f_v)
                except Exception as e:
                    # ExceptionInfo(e)
                    f_v = None
                    pass
            f_v = {} if f_v == '{}' else f_v

        # 默认值处理
        if f_v is None and pattern[3] is not None:
            f_v = pattern[3]
        return f_v

    def replace_values(self, print_process=False):
        """
        对所有值进行标准化、清洗
        :return:
        """
        # 获取标准化结构后的数据
        # self.get_format_dim_2_content()

        dim_2_data = []

        def trans_value(value, pattern, **kwargs):

            if isinstance(value, list):
                fvs = []
                for val in value:
                    f_v = trans_value(val, pattern, **kwargs)
                    fvs.append(f_v)
                return fvs
            else:
                # raise TypeError('this type of value must in (str, list).')
                return self.engine_for_data_clean(value, pattern, **kwargs)
            pass

        for d in self.format_dim_2_content:
            # p = self.patterns[k]
            k, v = d[0], d[1]
            fv = trans_value(v, self.patterns[k], standard_key=k)
            dim_2_data.append([k, fv])
            if print_process and v != fv:
                print('{} replace: {} => {}'.format(k, v, fv))

        # 对取值进行清洗后，重新赋值
        self.format_dim_2_content = dim_2_data
        pass

    def format_dim_2_content_to_dict(self):
        """
        还原铺平后的content成一个dict，这样做是为了
        可读性。其中主要的难点是数组类型的还原，很难
        推断多层的dict对象，哪一层、哪一个字段下是list，
        这个过程用到了一个关键信号，“序号”，在原始的
        数据结构有，有“序号”这个字段的，说明“序号”所在
        的dict一定在一个数组里面，
        但数组里面不一定有“序号”这个字段，例如基本信息
        模块下的变更记录是一个list对象,里面每个对象都
        有一个“序号”,但是变更记录里面的一条变更记录可能
        又有涉及的链接，一条变更记录可能涉及多个链接，
        所以，这些链接是放在一个数组里面的，但是并没有
        序号，为了解决这个问题，在content平面化的时候
        对于list类型的对象，会检查是不是有“序号”这个
        字段，若没有者添加。这样基本可以标记到原始content
        中有list的地方。
        另外就是需要还原成list的对象，下面list的长度
        不一致的问题
        :return:
        """
        jt = JsonTree.create_tree_from_list(
            self.format_dim_2_content
        )
        cnt = jt.to_format_dict(with_data=True)
        self.format_content = cnt
        pass
