import json
import math
import os
import time
import numpy as np
import pandas as pd


def get_data_url(input_dir_flow, start_year, start_month, end_year, end_month):
    pattern = input_dir_flow + "/yellow_tripdata_%d-%02d.csv"

    data_url = []

    i = start_year
    while i <= end_year:
        j = start_month if i == start_year else 1
        end_j = end_month if i == end_year else 12

        while j <= end_j:
            data_url.append(pattern % (i, j))
            j += 1

        i += 1

    return data_url


def judge_id(value, dividing_points, equally=True):
    if equally:
        min_v = dividing_points[0]
        interval = dividing_points[1] - dividing_points[0]
        idx = int((value - min_v) / interval)
        max_id = len(dividing_points) - 2
        return min(max_id, idx)
    else:
        for i, num in enumerate(dividing_points):
            if value <= num:
                return i - 1
        return len(dividing_points)


def gen_config_geo():
    geo = {
        "including_types": [
            "Polygon"
        ],
        "Polygon": {
        }
    }
    return geo


def gen_config_dyna():
    dyna = {
        "including_types": [
            "state"
        ],
        "state": {
            "entity_id": "geo_id",
            "inflow": "num",
            "outflow": "num"
        }
    }
    return dyna


def gen_config_info(file_name, interval):
    info = \
        {
            "data_col": [
                "inflow",
                "outflow"
            ],
            "data_files": [
                file_name
            ],
            "geo_file": file_name,
            "output_dim": 2,
            "init_weight_inf_or_zero": "inf",
            "set_weight_link_or_dist": "dist",
            "calculate_weight_adj": False,
            "weight_adj_epsilon": 0.1,
            "time_intervals": interval
        }
    return info


def gen_config(output_dir_flow, file_name, interval):
    config = {}
    data = json.loads(json.dumps(config))
    data["geo"] = gen_config_geo()
    data["dyna"] = gen_config_dyna()
    data["info"] = gen_config_info(file_name, interval)
    config = json.dumps(data)
    with open(output_dir_flow + "/config.json", "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)
    print(config)


if __name__ == '__main__':
    start_time = time.time()
    interval = 3600
    # 开始年月
    (start_year, start_month, start_day) = (2020, 4, 1)
    # 结束年月
    (end_year, end_month, end_day) = (2020, 4, 30)

    file_name = 'NYCTAXI%d%02d-%d%02d' % (start_year, start_month, end_year, end_month)
    output_dir_flow = 'output/NYCTAXI%d%02d-%d%02d_dyna' % (start_year, start_month, end_year, end_month)
    input_dir_flow = 'input/NYC-Taxi'
    data_url = get_data_url(input_dir_flow=input_dir_flow,
                            start_year=start_year,
                            start_month=start_month,
                            end_year=end_year,
                            end_month=end_month
                            )
    data_url = tuple(data_url)
    print(data_url)
    if not os.path.exists(output_dir_flow):
        os.makedirs(output_dir_flow)

    dataset_nyc = pd.concat(map(lambda x: pd.read_csv(x, index_col=False, usecols=[1, 2, 7, 8]), data_url), axis=0)
    dataset_nyc.reset_index(drop=True, inplace=True)
    print(dataset_nyc.shape)
    print(dataset_nyc.dtypes)
    # dataset_nyc = dataset_nyc[['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'PULocationID', 'DOLocationID']]
    # print(dataset_nyc.shape)

    # 筛除非法时间
    dataset_nyc = dataset_nyc.loc[
        dataset_nyc['tpep_pickup_datetime'].apply(
            lambda x:'%d-%02d-%02d' % (end_year, end_month, end_day) >= x[:10] >=
                     '%d-%02d-%02d' % (start_year, start_month, start_day))]
    dataset_nyc = dataset_nyc.loc[
        dataset_nyc['tpep_dropoff_datetime'].apply(
            lambda x:'%d-%02d-%02d' % (end_year, end_month, end_day) >= x[:10] >=
                     '%d-%02d-%02d' % (start_year, start_month, start_day))]
    print('after time selection, shape:', dataset_nyc.shape)
    # 筛选起点和终点不相等的记录
    dataset_nyc = dataset_nyc[dataset_nyc['PULocationID'] != dataset_nyc['DOLocationID']]
    print('after start!=end selection, shape:', dataset_nyc.shape)

    # 转时间戳
    print('begin to convert time string to timestamp')
    dataset_nyc['start_timestamp'] = dataset_nyc.apply(
        lambda x: time.mktime(time.strptime(x['tpep_pickup_datetime'], '%Y-%m-%d %H:%M:%S')), axis=1)
    dataset_nyc['end_timestamp'] = dataset_nyc.apply(
        lambda x: time.mktime(time.strptime(x['tpep_dropoff_datetime'], '%Y-%m-%d %H:%M:%S')), axis=1)
    print('finish converting time string to timestamp, shape:', dataset_nyc.shape)
    min_timestamp = min(dataset_nyc['start_timestamp'].min(), dataset_nyc['end_timestamp'].min())
    max_timestamp = max(dataset_nyc['start_timestamp'].max(), dataset_nyc['end_timestamp'].max())
    min_timestamp = float(math.floor(min_timestamp / interval) * interval)
    max_timestamp = float(math.ceil(max_timestamp / interval) * interval)
    # 按照时间间隔分段
    time_dividing_point = list(np.arange(min_timestamp, max_timestamp, interval))
    convert = []
    for t in time_dividing_point:
        convert.append(time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(t)))

    # 计算每个时间属于哪一段
    dataset_nyc['start_time_id'] = dataset_nyc.apply(
        lambda x: judge_id(x['start_timestamp'], time_dividing_point), axis=1)
    dataset_nyc['end_time_id'] = dataset_nyc.apply(
        lambda x: judge_id(x['end_timestamp'], time_dividing_point), axis=1)
    print('add time_id, shape:', dataset_nyc.shape)
    # 起点跟终点不在一个时间戳内
    dataset_nyc = dataset_nyc.loc[dataset_nyc['start_time_id'] != dataset_nyc['end_time_id']]
    print('time selection, shape:', dataset_nyc.shape)

    # 统计所有区域id，并生成转化的字典
    print('collecting area ids...')
    area = set()
    old2new = dict()
    for idx in dataset_nyc['PULocationID']:
        if idx not in area:
            old2new[idx] = len(area)
            area.add(idx)
    for idx in dataset_nyc['DOLocationID']:
        if idx not in area:
            old2new[idx] = len(area)
            area.add(idx)
    print('size of dyna_data:', (len(area), len(time_dividing_point) - 1, 2))
    dyna_data = np.zeros((len(area), len(time_dividing_point), 2))
    print('dyna calculating...')
    for i in range(dataset_nyc.shape[0]):
        # print(str(i) + '/' + str(dataset_nyc.shape[0]))
        start_time_id = dataset_nyc.iloc[i]['start_time_id']
        end_time_id = dataset_nyc.iloc[i]['end_time_id']
        start_geo_id = old2new[dataset_nyc.iloc[i]['PULocationID']]
        end_geo_id = old2new[dataset_nyc.iloc[i]['DOLocationID']]
        if start_time_id == end_time_id or start_geo_id == end_geo_id:
            continue
        # in++
        dyna_data[end_geo_id][end_time_id][0] = dyna_data[end_geo_id][end_time_id][0] + 1
        # out++
        dyna_data[start_geo_id][start_time_id][1] = dyna_data[start_geo_id][start_time_id][1] + 1
    # np.save('nyc_taxi_od.npy', od_data)
    # print('Saved nyc_taxi_od.npy.')
    # 输出
    data_name = output_dir_flow + "/" + file_name
    print('.geo outputing...')
    geo_data = pd.DataFrame(columns=['geo_id', 'type', 'coordinates'])
    geo_data['geo_id'] = list(range(len(area)))
    geo_data.loc[:, 'type'] = 'state'
    geo_data.loc[:, 'coordinates'] = '[[ [] ]]'
    geo_data.to_csv(data_name + '.geo', index=False)
    print('.od outputing...')
    dyna_id = 0
    dyna_file = open(data_name + '.dyna', 'w')
    dyna_file.write('dyna_id,' + 'type,' + 'time,' + 'entity_id,' + 'inflow,' + 'outflow' + '\n')
    for area_id in range(dyna_data.shape[0]):
        for time_id in range(dyna_data.shape[1]):
            time_str = convert[time_id]
            dyna_file.write(str(dyna_id) + ',' + 'state' + ',' + str(time_str)
                            + ',' + str(area_id)
                            + ',' + str(dyna_data[area_id][time_id][0])
                            + ',' + str(dyna_data[area_id][time_id][1]) + '\n')
            dyna_id = dyna_id + 1
    dyna_file.close()
    print('finish！')
    gen_config(output_dir_flow, file_name, interval)
    end_time = time.time()
    print(end_time - start_time)
