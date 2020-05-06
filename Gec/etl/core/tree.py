# encoding: utf-8

"""
project = zlr数据处理
file_name = tree
author = Administrator
datetime = 2020/4/30 0030 下午 22:36
from = office desktop
"""
from treelib import Tree


class JsonTree(Tree):

    def __init__(self, tree=None, deep=False,
                 node_class=None, identifier=None):
        Tree.__init__(self, tree, deep, node_class, identifier)
        pass

    def to_format_dict(self, nid=None, key=None, sort=True,
                       reverse=False, with_data=False):
        """Transform the whole tree into a dict."""

        nid = self.root if (nid is None) else nid
        ntag = self[nid].tag
        tree_dict = {ntag: {}}

        if self[nid].expanded:
            queue = [self[i] for i in self[nid].successors(self._identifier)]
            key = (lambda x: x) if (key is None) else key
            if sort:
                queue.sort(key=key, reverse=reverse)

            for elem in queue:
                _ = self.to_format_dict(
                    elem.identifier, with_data=with_data,
                    sort=sort, reverse=reverse)
                tree_dict[ntag] = dict(tree_dict[ntag], **_)
            if len(tree_dict[ntag]) == 0:
                tree_dict = self[nid].tag if not with_data else \
                    {ntag: self[nid].data}
            return tree_dict

    @staticmethod
    def create_tree_from_list(data):
        tree = JsonTree()
        for d in data:
            path = d[0].split('-')
            if len(path) > 2:
                parent = path[0]
                if tree.get_node(parent) is None:
                    tree.create_node(parent, parent)
                for i in range(1, len(path) - 1):
                    n = parent + '-' + path[i]
                    if tree.get_node(n) is None:
                        tree.create_node(path[i], n, parent=parent)
                    parent = n
                n = parent + '-' + path[-1]
                tree.create_node(path[-1], n, parent=parent, data=d[1])
            elif len(path) == 2:
                parent = path[0]
                if tree.get_node(parent) is None:
                    tree.create_node(parent, parent)
                n = parent + '-' + path[-1]
                tree.create_node(path[-1], n, parent=parent, data=d[1])
            elif len(path) == 1:
                parent = path[0]
                if tree.get_node(parent) is None:
                    tree.create_node(parent, parent, data=d[1])
            else:
                pass
        return tree
