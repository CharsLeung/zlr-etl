# encoding: utf-8

"""
project = zlr数据处理
file_name = run
author = Administrator
datetime = 2020/5/6 0006 下午 14:34
from = office desktop
"""
from Calf.data import BaseModel
from Gec.etl.base import Enterprise
from Gec.etl.develop import Develop
from Gec.etl.judicature import Judicature
from Gec.etl.risk import Risk
from Gec.etl.opt import Operating
from Gec.etl.right import Right
from Gec.etl.news import News
from Gec.etl.utils import progress_bar


def run():
    bm = BaseModel(
        tn='qcc_original'
    )
    bm2 = BaseModel(tn='qcc_format')

    metaModels = [
        '基本信息',
        '企业发展',
        '法律诉讼',
        '经营风险',
        '经营状况',
        '公司新闻',
        '知识产权'
    ]
    models = {
        '基本信息': Enterprise(), '企业发展': Develop(),
        '法律诉讼': Judicature(), '经营风险': Risk(),
        '经营状况': Operating(), '公司新闻': News(),
        '知识产权': Right()
    }
    for m in metaModels:
        enterprises = bm.query(
            sql={
                'metaModel': m,
                # 'name': '重庆斯麦尔酒店有限公司'
            },
            # field={'content': 1, '_id': 0},
            # no_cursor_timeout=True
            limit=1000,
            # skip=0
        )
        print('\ndeal metaModel({})...'.format(m))
        mdl = models[m]
        mdl.run(enterprises, bm2)
    pass


run()


def duplication():
    import time
    import pandas as pd

    bm = BaseModel(tn='qcc')

    metaModels = [
        '基本信息',
        # '企业发展',
        # '法律诉讼',
        # '经营风险',
        # '经营状况',
        # '公司新闻',
        # '知识产权'
    ]

    for m in metaModels:
        data = bm.aggregate(
            pipeline=[
                {'$match': {'metaModel': m}},
                {'$project': {
                    '_id': 0,
                    'name': 1,
                    # 'recall': 1,
                    # 'date': 1
                }}
            ]
        )
        data = pd.DataFrame(list(data))
        data.to_csv('qcc_names.csv', index=False)
        # data = data.sort_values(['name', 'recall', 'date'], ascending=False)
        # data['dup'] = data['name'].duplicated(keep='first')
        # total = len(data)
        # dup = data[data['dup']]['_id']
        # dup_count = len(dup)
        # print('\nduplicate({}): {}/{}'.format(m, dup_count, total))
        # i = 0
        # start = time.time()
        # for _ in dup:   # duplicate: 356454/1691737
        #     # bm.remove(_id=i)
        #     dc = bm.mc.delete_one({'_id': _})
        #     i += dc.deleted_count
        #     if i % 10 == 0:
        #         progress_bar(
        #             dup_count, i, 'drop duplicate data and spend {} '
        #                           'seconds'.format(int(time.time() - start)))
        pass


# duplication()