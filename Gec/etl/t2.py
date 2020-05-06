# encoding: utf-8

"""
project = zlr数据处理
file_name = t2
author = Administrator
datetime = 2020/4/27 0027 下午 17:32
from = office desktop
"""
from treelib import Node, Tree


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


# ds = [['A-B', 1], ['A-C', 4]]
# tr = pathTree(ds)
# new_tree = Tree()
# new_tree.create_node("n1", 1)  # root node
# new_tree.create_node("n3", 2, parent=1, data='a')
# new_tree.create_node("n3", 3, parent=1, data='b')
# d = new_tree.to_dict_format(with_data=True)

# for n in tr.nodes:
#     lvs = tr.leaves(n)
#     pass
# import numpy as np
#
# print(np.array([1, [1, 2], [1, [2, 3]]]).shape)

pass

d1 = {'n1': {'n3': 1}}
d2 = {'n3': 1}
pass


