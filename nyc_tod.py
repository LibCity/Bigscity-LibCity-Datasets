import csv
import json
import os.path

import numpy as np


def gen_config_gridod(row_num, column_num):
    gridod = {
        "including_types": [
            "state"
        ], "state": {
            "row_id": row_num,
            "column_id": column_num,
            "flow": "num",
        }
    }
    return gridod


def gen_config_geo():
    geo = {"including_types": [
        "Point"
    ],
        "Point": {
            "row_id": "num",
            "column_id": "num"
        }
    }
    return geo


def gen_config_info(file_name, interval):
    info = \
        {
            "data_col": [
                "flow"
            ],
            "data_files": [
                file_name
            ],
            "geo_file": file_name,
            "ext_file": file_name,
            "output_dim": 1,
            "init_weight_inf_or_zero": "inf",
            "set_weight_link_or_dist": "dist",
            "calculate_weight_adj": False,
            "weight_adj_epsilon": 0.1,
            "time_intervals": interval,
        }
    return info


def gen_config_ext():
    ext = \
        {
            "including_types": [
                ""
            ]
        }
    return ext


def gen_config(output_dir_flow, file_name, row_num, column_num, interval):
    config = {}
    data = json.loads(json.dumps(config))
    data["geo"] = gen_config_geo()
    data["ext"] = gen_config_ext()
    data["gridod"] = gen_config_gridod(row_num, column_num)
    data["info"] = gen_config_info(file_name, interval)
    config = json.dumps(data)
    with open(output_dir_flow + "/config.json", "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)
    print(config)


if __name__ == '__main__':

    dataset = 'NYC_TOD'
    output_dir = 'output/'
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    # 处理 OD 数据流信息
    print("Start processing OD data...")
    od_data_npy = 'oddata.npy'
    od_data = np.load(od_data_npy, allow_pickle=True, encoding='latin1')[()][0]  # (T, R*C, R, C)
    row_num = od_data.shape[2]
    column_num = od_data.shape[3]

    dyna_file = open(output_dir + '/' + dataset + '.gridod', 'w')
    writer = csv.writer(dyna_file)
    writer.writerow(
        ['dyna_id', 'type', 'time',
         'origin_row_id', 'origin_column_id', 'destination_row_id', 'destination_column_id',
         'flow'])

    for idx, line in enumerate(od_data):
        for origin_row in range(row_num):
            for origin_column in range(column_num):
                for destination_row in range(row_num):
                    for destination_column in range(column_num):
                        writer.writerow([idx,
                                         'state',
                                         '',
                                         origin_row,
                                         origin_column,
                                         destination_row,
                                         destination_column,
                                         line[origin_row * column_num + origin_column][destination_row][
                                             destination_column]
                                         ])
    dyna_file.close()
    print("Finish processing OD data.")

    # 生成 GEO 信息
    print('Start generating geo file...')
    geo_file = open(output_dir + '/' + dataset + '.geo', 'w')
    writer = csv.writer(geo_file)
    writer.writerow(['geo_id', 'type', 'coordinates', 'row_id', 'column_id'])
    for r in range(row_num):
        for c in range(column_num):
            writer.writerow([r * row_num + c, 'Point', '[]', r, c])
    geo_file.close()
    print('Finish generating geo file.')

    # 处理外部数据特征
    print("Start processing ext data...")

    weather_npy = 'weather.npy'
    weather = np.load(weather_npy, allow_pickle=True, encoding='latin1')[()][0]  # (T, F)

    ext_file = open(output_dir + '/' + dataset + '.ext', 'w')
    writer = csv.writer(ext_file)
    writer.writerow(
        ['ext_id', 'time',
         0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28])
    for idx, line in enumerate(weather):
        writer.writerow([idx, ''] + list(line))

    ext_file.close()
    print("Finish processing ext data.")

    # 生成信息文件
    print("Start processing config file...")
    gen_config(output_dir, file_name=dataset, row_num=row_num, column_num=column_num, interval=1800)
    print("Finish processing config file.")
