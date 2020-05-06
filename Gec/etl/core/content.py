# encoding: utf-8

"""
project = zlr数据处理
file_name = content
author = Administrator
datetime = 2020/4/29 0029 上午 9:00
from = office desktop
"""
import re
import pandas as pd
import datetime as dt

from Gec.exception import SuccessMessage, \
    WarningMessage, ErrorMessage, ExceptionInfo
from Gec.etl.utils import JsonPath, dictMerge, \
    dictTranspose2List, pathTree


class Content(object):
    types = {
        'int': lambda x: int(x) if x is not None else x,
        'float': lambda x: float(x) if x is not None else x,
        'str': lambda x: str(x) if x is not None else x,
    }

    match = {}

    # patterns属性是一个关键的数据，它代表了数据清洗的关键
    # 它应该是一个二维数组，每个元素有三个子元素，分别是：
    # 1.标准的字段域结构；2.针对字段域结构的reg；3.针对取值的reg
    patterns = {}

    def __init__(self, content=None):
        self.source_dim_2_content = []
        self.format_dim_2_content = []
        self.format_content = {}
        self.content = content
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
                self.source_dim_2_content.append(
                    '{}:{}'.format(root, {}))
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
                                _ = '{}{}{}:{}'.format(root, sep, k, sv)
                                self.source_dim_2_content.append(_)
                            else:
                                # if '序号' not in sv.keys():
                                sv['序号'] = 1
                                self.get_keys(
                                    v[0], '{}{}{}'.format(
                                        root, sep, '{}#1'.format(k)),
                                    sep, return_value, filter_key,
                                    keep_key, value_sep)
                        else:
                            xh = 1
                            for sv in v:
                                if not isinstance(sv, dict):
                                    _ = '{}{}{}:{}'.format(root, sep, k, sv)
                                    self.source_dim_2_content.append(_)
                                else:
                                    # if '序号' not in sv.keys():
                                    sv['序号'] = xh
                                    xh += 1
                                    self.get_keys(
                                        sv, '{}{}{}'.format(
                                            root, sep, '{}#{}'.format(k, sv['序号'])),
                                        sep, return_value, filter_key,
                                        keep_key, value_sep)
                            pass

                    else:
                        self.source_dim_2_content.append(
                            '{}{}{}:{}'.format(root, sep, k, {}))
                        pass
                else:
                    if return_value:
                        self.source_dim_2_content.append(
                            '{}{}{}:{}'.format(root, sep, k, v))
                        # ks = []

                    pass
        else:
            self.source_dim_2_content.append('{}:{}'.format(root, _))
        # return category

    @staticmethod
    def group(data):
        # ds = pd.DataFrame(data=data, columns=['k', 'v'])
        rp = re.compile(r'#(\d+)(?!.*#\d+)')
        jp = JsonPath(paths=[d[0] for d in data])

        def fillPath(ps):
            _ = []
            keys = [d[0] for d in ps]

            def fillSonPath(v, pp):
                _ds_ = []
                # if isinstance(pp, list):
                #     for p in pp:
                #         _ds_ += func(v, p)
                # else:
                if pp == '{}':
                    sp = jp.son_path(v)
                    if len(sp):
                        # 关键步骤，找到子目后，加上#1这个位置索引标识符
                        _ds_ = [[v + '#1' + s, ''] for s in sp]
                    else:
                        # 如果没有发现v的任何子目，把v这个
                        # 路径留到下一轮
                        _ds_ = [[v, '{}']]
                return _ds_

            def fillBrotherPath(v):
                _ds_ = []
                fp = '-'.join(v.split('-')[:-1])
                bp = jp.brother(d[0])
                if len(bp):
                    for b in bp:
                        fb = fp + b
                        if fb not in keys:
                            _ds_.append([fb, ''])
                return _ds_

            for d in ps:
                if '{}' in d[1]:
                    __ = fillSonPath(d[0], d[1])
                    if len(__):
                        _ += __
                elif '序号' in d[0]:
                    # TODO: 对于信息不全的，还要补充兄弟节点
                    _.append(d)
                    __ = fillBrotherPath(d[0])
                    if len(__):
                        _ += __
                else:
                    _.append(d)

            return _

        def convolution(ds):
            # 通过path全集，补全为{}的空数组，{}是程序加上去的
            # 原json是一个[]
            dds = fillPath(ds)
            # 先补全{}后，才能替换#\d
            dds = [[re.sub(rp, '', d[0]), d[1]] for d in dds]
            # __ = fillPath(dds)
            # _ = dictMerge({}, *ds, append=True)
            df = pd.DataFrame(data=dds, columns=['k', 'v'])
            # df['k'] = df.k.map(lambda x: re.sub(rp, '', x))
            df = df.groupby(['k'], as_index=False).agg({
                'v': lambda x: list(x) if len(x) > 1 else list(x)[0]
            })
            return df.values.tolist()
            pass
            # return [[k, v] for k, v in zip(_.keys(), _.values())]

        def convolutions(ds):
            ds2 = convolution(ds)
            while len(ds) != len(ds2):
                ds = ds2
                ds2 = convolution(ds)
            return ds2

        data = convolutions(data)
        # d2 = fillPath(d1)
        # if len(d1) != len(d2):
        #     d2 = convolutions(d2, False, True)

        return data

    def get_source_dim_2_content(self):
        """
        把多层结构的json数据转换成二维平面的数据
        :return:
        """
        # ds = get_keys(self.content, root='content',
        #               sep='-', return_value=True,
        #               value_sep='<tbl>')
        # self.content.pop('认证信息')
        # self.content.pop('工商信息')
        # self.content.pop('工商股东')
        # self.content.pop('对外投资')

        self.get_keys(self.content, root='content',
                      sep='-', return_value=True,
                      value_sep='<tbl>')
        ds = self.source_dim_2_content
        data = [[i.split(':')[0], ':'.join(
            i.split(':')[1:])] for i in ds]
        # 1.
        # 通过相同字段链a的下面的数据结构，补充字段
        # 链b的数据结构，字段链a与字段链b是相同的，同
        # 处一个数组里面，但字段链b下是一个空的结构，
        data = self.group(data)
        self.source_dim_2_content = data
        # return dim_2_data
        pass

    def get_format_dim_2_content(self):
        """
        1.把format_dim_2_content从一个list类型转
        化成dict类型
        :return:
        """
        data = self.format_dim_2_content

        dim_2_data = {}
        for d in data:
            # _ = d[1] if len(d[1]) > 1 else d[1][0]
            # 到这一步如果还是'{}'，那就是真的{}
            _ = {} if d[1] == '{}' else d[1]
            dim_2_data[d[0]] = _
        self.format_dim_2_content = dim_2_data
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

        match_type = None
        for d in self.source_dim_2_content:
            if d[0] in self.match.keys():
                mk = self.match[d[0]]
                for fk in mk:
                    dim_2_data.append([fk, d[1]])
                match_type = 'cache'
            else:
                ms = []
                for pk, pv in zip(self.patterns.keys(),
                                  self.patterns.values()):
                    fk = trans_key(d[0], pv[0], standard_key=pk)
                    if fk is not None:
                        dim_2_data.append([fk, d[1]])
                        # self.match[k] = fk
                        ms.append(fk)
                        match_type = 'regular'
                    pass
                if len(ms):
                    self.match[d[0]] = ms
                if len(ms) > 1:
                    print(WarningMessage('multiple replace for {}: {}.'
                                         ''.format(d[0], ms)))
            if d[0] in self.match.keys():
                if print_process:
                    print(SuccessMessage('replace({}): {} => {}'.format(
                        match_type, d[0], self.match[d[0]])))
                pass
            else:
                print(ErrorMessage('mismatch {}:{}'.format(
                    d[0], d[1])))
        self.format_dim_2_content = dim_2_data
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
        self.get_format_dim_2_content()

        dim_2_data = {}

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

        for k, v in zip(self.format_dim_2_content.keys(),
                        self.format_dim_2_content.values()):
            # p = self.patterns[k]
            fv = trans_value(v, self.patterns[k], standard_key=k)
            dim_2_data[k] = fv
            if print_process and v != fv:
                print('{} replace: {} => {}'.format(k, v, fv))

        # 对取值进行清洗后，重新赋值
        self.format_dim_2_content = dim_2_data
        pass

    def format_dim_2_content_to_json(self):
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
        content = {}

        def trans(cnt, last_layer_key):
            for layer in last_layer_key:
                rp = '["' + '"]["'.join(reversed(layer)) + '"]'
                _ = eval('cnt{}'.format(rp))
                # 关键一招
                _ds_ = []
                try:
                    T = dictTranspose2List(_)
                    ts = []
                    for __ in T:
                        # __ = T[i]
                        if '序号' in __.keys():
                            if isinstance(__['序号'], list):
                                try:
                                    _d_ = dictTranspose2List(__)
                                except ValueError:
                                    _d_ = __
                                ts.append(_d_)
                            else:
                                ts.append(__)
                        else:
                            ts.append(__)

                except ValueError as e:
                    ExceptionInfo(e)
                    ts = _
                    pass

                try:
                    exec('cnt{}={}'.format(rp, ts))
                except Exception as e:
                    ExceptionInfo(e)

        list_keys = []
        for k, v in zip(self.format_dim_2_content.keys(),
                        self.format_dim_2_content.values()):
            d = {}
            f = False
            p = []

            for _ in reversed(k.split('-')):
                d = {_: v} if len(d) == 0 else {_: d}
                if f:
                    p.append(_)
                if _ == '序号':
                    f = True
            if len(p):
                list_keys.append(p)
            dictMerge(content, d)
        list_keys.sort(key=lambda x: len(x), reverse=True)

        trans(content, list_keys)
        self.format_content = content
        return content
        pass

    def to_tree(self):
        ds = [[k, v] for k, v in zip(self.format_dim_2_content.keys(),
                                     self.format_dim_2_content.values())]
        pt = pathTree(ds)
        for n in pt.nodes:
            leaves = pt.leaves(n)
