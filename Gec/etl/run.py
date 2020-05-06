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


def run():
    bm = BaseModel(tn='qcc_original')
    bm2 = BaseModel(tn='qcc_format')

    metaModels = ['基本信息', '企业发展', '法律诉讼', '经营风险',
                  '经营状况', '公司新闻', '知识产权']
    models = {
        '基本信息': Enterprise, '企业发展': Develop,
        '法律诉讼': Judicature, '经营风险': Risk,
        '经营状况': Operating, '公司新闻': News,
        '知识产权': Right
    }
    for m in metaModels:
        enterprises = bm.query(
            sql={'metaModel': m},
            # field={'content': 1, '_id': 0},
            no_cursor_timeout=True)
        print('deal metaModel({})...'.format(m))
        mdl = models[m]
        mdl.run(enterprises, bm2)
    pass


run()
