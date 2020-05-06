# encoding: utf-8

"""
project = zlr数据处理
file_name = t1
author = Administrator
datetime = 2020/4/26 0026 下午 15:33
from = office desktop
"""
from Calf.data import BaseModel
from Calf.utils import File
from Gec.etl.utils import read_json
from Gec.etl.utils import dictToDim2
from Gec import workspace


def insert():
    bm = BaseModel(tn='qcc_original')
    fs = File.get_all_file('D:\graph_data\data\qcc_20200423\\')
    for f in fs:
        js = read_json(f)
        try:
            bm.insert_batch(js)
        except:
            continue
    pass


# js = read_json('D:\graph_data\data\qcc_20200421\\103.123.212.200_jguqomcwyh.json')
# insert()


def get_old_keys():
    bm = BaseModel(tn='qcc_original')

    metaModel = '公司新闻'

    enterprises = bm.query(
        sql={'metaModel': metaModel,
             # 'name': '重庆导宇科技有限公司'
             },
        field={'content': 1, '_id': 0, 'name': 1},
        no_cursor_timeout=True)
    i = 0
    exit_filed = set()
    for etp in enterprises:
        i += 1
        # if i > 10:
        #     break
        name = etp.pop('name')
        try:
            cs = dictToDim2(etp, metaModel, '$')
        except Exception as e:
            print(e)
            print(name)
        for c in cs:
            exit_filed.add(c)
        pass

    data = []
    for s in exit_filed:
        _ = s.split('$')
        d = []
        for i in _:
            if len(i):
                d.append(i)
        data.append(','.join(d) + '\n')
    fp = workspace + '{}\\'.format(metaModel)
    File.check_file(fp)
    with open(fp + '字段.csv', 'w', encoding='gbk') as f:
        f.writelines(data)
        pass
    # exit_filed = pd.DataFrame(data=[f for f in exit_filed], columns=['key'])
    # fp = workspace + '{}\\'.format(metaModel)
    # File.check_file(fp)
    # exit_filed.to_csv(fp + '字段.csv', index=False)
    pass


get_old_keys()