# encoding: utf-8

"""
project = zlr(20200403备份)
file_name = utils
author = Administrator
datetime = 2020/4/23 0023 上午 9:15
from = office desktop
"""
import re
import json

from treelib import Node, Tree
from Gec.exception import ExceptionInfo


def read_json(path):
    """

    :param path:
    :return:
    """
    try:
        with open(path, encoding='utf-8') as file:
            i = 1
            result = []
            try:
                for line in file:
                    try:
                        result.append(json.loads(line))
                    except Exception as e:
                        ExceptionInfo(e)
                        print('Exception on ({}, line {})'.format(
                            path, i
                        ))
                    i += 1
            except Exception as e:
                ExceptionInfo(e)
                print('Exception on ({}, line {})'.format(
                    path, i
                ))
            ds = result
            # ds = [json.loads(line) for line in file]
        return ds
    except Exception as e:
        ExceptionInfo(e)
        print(e, path)
        return []


def dictToDim2(_, root='', sep='-', return_value=False,
               filter_key=[], keep_key=[], value_sep='\n'):
    """
    从一个结构层次比较复杂的dict中分析出key的结构层次，
        默认以'-'分割上下依赖关系
    :param _:
    :param root:
    :param sep:
    :param return_value:
    :param filter_key:
    :param keep_key:
    :param value_sep:
    :return:
    """
    dim_2_data = []
    if isinstance(_, list):
        if len(_):
            for sv in _:
                dim_2_data += dictToDim2(
                    sv, root, sep, return_value,
                    filter_key, keep_key, value_sep
                )
        else:
            if return_value:
                _ = '{}:{}'.format(root, {})
            else:
                _ = '{}'.format(root)
            dim_2_data.append(_)
            pass
    elif isinstance(_, dict):
        # keep = True if len(keep_key) else False
        for k, v in zip(_.keys(), _.values()):
            if k in filter_key:
                continue
            # if keep:
            #     for kp in keep_key:
            #         if kp not in '{}{}'.format(root, k):
            #             continue
            # print('{}-{}'.format(root, k))
            if isinstance(v, dict):
                dim_2_data += dictToDim2(
                    v, '{}{}{}'.format(root, sep, k),
                    sep, return_value, filter_key,
                    keep_key, value_sep)
                pass
            elif isinstance(v, list):
                if len(v):
                    if len(v) == 1:
                        sv = v[0]
                        if not isinstance(sv, dict):
                            if return_value:
                                _ = '{}{}{}:{}'.format(root, sep, k, sv)
                            else:
                                _ = '{}{}{}'.format(root, sep, k)
                            dim_2_data.append(_)
                        else:
                            if '序号' not in sv.keys():
                                sv['序号'] = 1
                            # '{}#1'.format(k)
                            dim_2_data += dictToDim2(
                                v[0], '{}{}{}'.format(root, sep, k),
                                sep, return_value, filter_key,
                                keep_key, value_sep)
                    else:
                        xh = 1
                        for sv in v:
                            if not isinstance(sv, dict):
                                if return_value:
                                    _ = '{}{}{}:{}'.format(root, sep, k, sv)
                                else:
                                    _ = '{}{}{}'.format(root, sep, k)
                                dim_2_data.append(_)
                            else:
                                if '序号' not in sv.keys():
                                    sv['序号'] = xh
                                    xh += 1
                                # '{}#{}'.format(k, sv['序号'])
                                dim_2_data += dictToDim2(
                                    sv, '{}{}{}'.format(root, sep, k),
                                    sep, return_value, filter_key,
                                    keep_key, value_sep)

                else:
                    if return_value:
                        _ = '{}{}{}:{}'.format(root, sep, k, {})
                    else:
                        _ = '{}{}{}'.format(root, sep, k)
                    dim_2_data.append(_)
                    pass
            else:
                if return_value:
                    _ = '{}{}{}:{}'.format(root, sep, k, {})
                else:
                    _ = '{}{}{}'.format(root, sep, k)
                dim_2_data.append(_)
                pass
    else:
        if return_value:
            _ = '{}:{}'.format(root, {})
        else:
            _ = '{}'.format(root)
        dim_2_data.append(_)
        pass
    return dim_2_data


def dictMerge(dic1, *args, append=True):
    """
    嵌套字典合并, 将新字典合并到旧字典中,新旧字典
    将自顶向下合并，不会将新字典合并到旧字典的子树
    当中去，例如eg3.的结果不会是：
    {'A': {'A1': {'B1': 1, 'B2': 3}, 'A2': 2}}
    eg1.
    >>> d1={'A': 1, 'B': 2}
    >>> d2={'A': 2, 'B': 2}
    >>> dictMerge(d1, d2)
    >>> d1
    {'A': [1, 2], 'B': 2}
    eg2.
    >>> d1={'A':{'A1':1, 'A2':2}}
    >>> d2={'A':{'A1':1, 'B2':2}}
    >>> dictMerge(d1, d2)
    >>> d1
    {'A': {'A1': [1, 1], 'A2': 2, 'B2': 2}}
    eg3.
    >>> d1={'A':{'A1':{'B1': 1}, 'A2':2}}
    >>> d2={'A1':{'B2':3}}
    >>> dictMerge(d1, d2)
    >>> d1
    {'A': {'A1': {'B1': 1}, 'A2': 2}, 'A1': {'B2': 3}}
    :param append:
    :param dic1: 旧字典
    :param args: 新字典
    :return:
    """
    for dic2 in args:
        for i in dic2:
            if i in dic1:
                # 如果i在原来的dict当中
                if isinstance(dic1[i], dict) and isinstance(
                        dic2[i], dict):
                    dictMerge(dic1[i], dic2[i])
                else:
                    if isinstance(dic1[i], list) and append:
                        dic1[i].append(dic2[i])
                    else:
                        dic1[i] = [dic1[i], dic2[i]]
            else:
                dic1[i] = dic2[i]
    return dic1


# d1 = {'A':{'A1':{'B1': 1}, 'A2':2}}
# d2 = {'A1':{'B2':3}}
# dictMerge(d1, d2)
# print(dictMerge(d1, d2))


def dictTranspose2List(dct):
    """
    把一个dict看做一颗树，这颗树的叶节点取值为长度相
    等的list，此函数将这颗树转化成多颗树，转换后的树
    的叶节点取值是原来list中的一个值。
    dct是一个dict，将取值类型为list的节点视为叶节点，
    list当中的值视为最小单位，不能再往下分割，叶节点
    的长度相等，此过程从上到下进行
    eg1.
    >>> dct = {'A': {
    ...            'A1':['x1', 'x2'],
    ...            'A2':['n1', 'n2']
    ...        },
    ...        'B':['m1','m2']
    ...    }
    >>> _ = dictTranspose2List(dct)
    >>> print(_)
    [{'A': {'A1':'x1','A2':'n1'}，'B':'m1'},
     {'A': {'A1':'x2','A2':'n2'}，'B':'m2'}
    ]
    eg2.
    >>> dct = {'A': [{
    ...            'A1':'x1',
    ...            'A2':'n1'
    ...        },{
    ...            'A1':'x1',
    ...            'A2':'n1'
    ...        }],
    ...        'B':['m1','m2']
    ...    }
    >>> _ = dictTranspose2List(dct)
    >>> print(_)

    [{'A': {'A1': 'x1', 'A2': 'n1'}, 'B': 'm1'},
     {'A': {'A1': 'x1', 'A2': 'n1'}, 'B': 'm2'}
    ]
    eg3.
    >>> dct = {'A': [{
    ...            'A1':['x1', 'x2'],
    ...            'A2':['n1', 'n2']
    ...        },{
    ...            'A1':['x1-', 'x2-'],
    ...            'A2':['n1-', 'n2-']
    ...        }],
    ...        'B':['m1','m2']
    ...    }
    >>> _ = dictTranspose2List(dct)
    >>> print(_)

    [{'A': {'A1': ['x1', 'x2'], 'A2': ['n1', 'n2']}, 'A1': 'm1'},
     {'A': {'A1': ['x1-', 'x2-'], 'A2': ['n1-', 'n2-']}, 'A1': 'm2'}
     ]
    需要注意到eg3的例子中A.A1下的['x1', 'x2']并没有
    被拆开，因为A1所在的层是一个list层，A1层被当做叶
    节点，没有继续往下分了，其他几个位置也是一样的。
    :param dct: dict
    :return: list or ValueError
    """

    def func(dic, root=None):
        ds = []
        if isinstance(dic, dict):
            for k_, v_ in zip(dic.keys(), dic.values()):
                if isinstance(v_, list):
                    d = []
                    for i in range(len(v_)):
                        if root is None:
                            d.append({k_: v_[i]})
                        else:
                            d.append({root: {k_: v_[i]}})
                        # dict(**{k_: v_[i]})
                    ds.append(d)
                elif isinstance(v_, dict):
                    ds += func(v_, k_)
                else:
                    if root is None:
                        ds.append([{k_: v_}])
                    else:
                        ds.append([{root: {k_: v_}}])
                    pass
        return ds
        pass

    grid = func(dct)
    if len(grid):
        # 转置
        shape = [len(g) for g in grid]
        if max(shape) != min(shape):
            raise ValueError('non-standard data format.')
        grid = [[row[i] for row in grid] for i in range(len(grid[0]))]
        T = []
        for g in grid:
            if len(g) > 1:
                _ = dictMerge(g[0], *g[1:])
            else:
                _ = g[0]
            T.append(_)
        # _ = [dictMerge(g[0], *g[1:]) for g in grid]
        return T
    else:
        return []


# dd = {'A': [{
#     'A1': ['x1', 'x2'],
#     'A2': ['n1', 'n2']
#     }, {
#     'A1': ['x1-'],
#     'A2': ['n1-']
#     }],
#     'A1': [
#         ['m1', 'm11'],
#         'm2']
# }
#
# print(dictTranspose2List({'A': {'C': 1}, "B": {'C': 2}}))
# pass


class JsonPath:
    rp = re.compile(r'#\d+')

    def __init__(self, paths):
        self.paths = list(set([re.sub(self.rp, '', p) for p in paths]))
        pass

    def father_path(self, path):
        """
        父级路径，json这种结构中，父路径只会有一个
        :param path:
        :return:
        """
        for p in self.paths:
            if len(p) > len(path) and path in p:
                return

    def son_path(self, path):
        """
        子路径
        :param path:
        :return:
        """
        ps = []
        # fp = '-'.join(path.split('-')[:-1])
        fp = re.sub(self.rp, '', path)
        for p in self.paths:
            if len(p) > len(fp) and fp in p:
                ps.append(p.replace(fp, ''))
        return ps

    def brother(self, path):
        ps = []
        path = re.sub(self.rp, '', path)
        _ = path.split('-')
        fp = '-'.join(_[:-1])
        # fp = path
        for p in self.paths:
            if len(p) > len(fp) and fp in p and p != path:
                ps.append(p.replace(fp, ''))
        return ps


def pathTree(data):
    tree = Tree()
    for d in data:
        path = d[0].split('-')
        if len(path) > 2:
            parent = path[0]
            if tree.get_node(parent) is None:
                tree.create_node(parent, parent)
            for i in range(1, len(path)-1):
                n = parent + '-' + path[i]
                if tree.get_node(n) is None:
                    tree.create_node(path[i], n, parent=parent)
                parent = n
            n = parent + '-' + path[-1]
            tree.create_node(path[-1], n, parent=parent, data=d[1])

    return tree


def progress_bar(total, complete, info):
    isr = int(60 * complete / total)
    sr = '|' * isr
    print('\rRun:{0}'.format(info), end=' ', flush=True)
    print('\033[7;37;30m {0} \033[0m'.format(sr), end=' ', flush=True)
    print('{0}/{1}'.format(complete, total), end=' ', flush=True)
    pass