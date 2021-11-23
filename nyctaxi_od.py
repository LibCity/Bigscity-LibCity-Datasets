# link: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
import json
import math
import os
import time
import numpy as np
import pandas as pd


def rad(d):
    return d * math.pi / 180.0


def dis(lng1, lat1, lng2, lat2):  # 经度1 纬度1 经度2 纬度2
    EARTH_RADIUS = 6378.137
    radLat1 = rad(lat1)
    radLat2 = rad(lat2)
    a = radLat1 - radLat2
    b = rad(lng1) - rad(lng2)
    s = 2 * math.asin(math.sqrt(math.pow(math.sin(a / 2), 2) +
                    math.cos(radLat1) * math.cos(radLat2) * math.pow(math.sin(b / 2), 2)))
    s = s * EARTH_RADIUS
    return round(s, 3)


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


def gen_config_od():
    od = {
        "including_types": [
            "state"
        ],
        "state": {
            "origin_id": "geo_id",
            "destination_id": "geo_id",
            "flow": "num"
        }
    }
    return od


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
            "rel_file": file_name,
            "output_dim": 1,
            "init_weight_inf_or_zero": "inf",
            "set_weight_link_or_dist": "dist",
            "calculate_weight_adj": True,
            "weight_adj_epsilon": 0.1,
            "time_intervals": interval
        }
    return info


def gen_config(output_dir_flow, file_name, interval):
    config = {}
    data = json.loads(json.dumps(config))
    data["geo"] = gen_config_geo()
    data['od'] = gen_config_od()
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
    (end_year, end_month, end_day) = (2020, 6, 30)

    file_name = 'NYCTAXI%d%02d-%d%02d' % (start_year, start_month, end_year, end_month)
    output_dir_flow = 'output/NYCTAXI%d%02d-%d%02d_OD' % (start_year, start_month, end_year, end_month)
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

    dataset_nyc = pd.concat(map(lambda x: pd.read_csv(x, index_col=False), data_url), axis=0)
    dataset_nyc.reset_index(drop=True, inplace=True)
    print(dataset_nyc.shape)
    dataset_nyc = dataset_nyc[['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'PULocationID', 'DOLocationID']]
    print(dataset_nyc.shape)
    # data_num = dataset_nyc.shape[0]
    # dataset_nyc["drive_id"] = list(range(data_num))
    # 筛除非法时间
    dataset_nyc = dataset_nyc.loc[dataset_nyc['tpep_pickup_datetime'].
        apply(lambda x:
              '%d-%02d-%02d' % (end_year, end_month, end_day) >= x[:10] >=
              '%d-%02d-%02d' % (start_year, start_month, start_day))]
    dataset_nyc = dataset_nyc.loc[dataset_nyc['tpep_dropoff_datetime'].
        apply(lambda x:
              '%d-%02d-%02d' % (end_year, end_month, end_day) >= x[:10] >=
              '%d-%02d-%02d' % (start_year, start_month, start_day))]
    print(dataset_nyc.shape)
    # 筛选起点和终点不相等的记录
    dataset_nyc = dataset_nyc[(dataset_nyc['PULocationID'] <= 263) & (dataset_nyc['PULocationID'] > 0)
                      & (dataset_nyc['DOLocationID'] <= 263) & (dataset_nyc['DOLocationID'] > 0)]
    print(dataset_nyc.shape)
    dataset_nyc = dataset_nyc[dataset_nyc['PULocationID'] != dataset_nyc['DOLocationID']]
    print(dataset_nyc.shape)
    # 转时间戳
    dataset_nyc['start_timestamp'] = dataset_nyc.apply(
        lambda x: time.mktime(time.strptime(x['tpep_pickup_datetime'], '%Y-%m-%d %H:%M:%S')), axis=1)
    dataset_nyc['end_timestamp'] = dataset_nyc.apply(
        lambda x: time.mktime(time.strptime(x['tpep_dropoff_datetime'], '%Y-%m-%d %H:%M:%S')), axis=1)
    print(dataset_nyc.shape)
    min_timestamp = min(dataset_nyc['start_timestamp'].min(), dataset_nyc['end_timestamp'].min())
    max_timestamp = max(dataset_nyc['start_timestamp'].max(), dataset_nyc['end_timestamp'].max())
    min_timestamp = float(math.floor(min_timestamp / interval) * interval)
    max_timestamp = float(math.ceil(max_timestamp / interval) * interval)
    # 按照时间间隔分段
    time_dividing_point = list(np.arange(min_timestamp, max_timestamp, interval))
    convert = []
    for t in time_dividing_point:
        convert.append(time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(t)))
    print(dataset_nyc.shape)
    # 计算每个时间属于哪一段
    dataset_nyc['start_time_id'] = dataset_nyc.apply(
        lambda x: judge_id(x['start_timestamp'], time_dividing_point), axis=1)
    dataset_nyc['end_time_id'] = dataset_nyc.apply(
        lambda x: judge_id(x['end_timestamp'], time_dividing_point), axis=1)
    print(dataset_nyc.shape)
    # 起点跟终点要在一个时间戳内
    dataset_nyc = dataset_nyc.loc[dataset_nyc['start_time_id'] == dataset_nyc['end_time_id']]
    print(dataset_nyc.shape)
    # area = set()
    # old2new = dict()
    # for idx in dataset_nyc['PULocationID']:
    #     if idx not in area:
    #         old2new[idx] = len(area)
    #         area.add(idx)
    # for idx in dataset_nyc['DOLocationID']:
    #     if idx not in area:
    #         old2new[idx] = len(area)
    #         area.add(idx)
    # 输出
    data_name = output_dir_flow + "/" + file_name

    location = json.load(open(input_dir_flow + '/taxi_zones_final.json', 'r'))
    id_list = []
    id2lon = {}
    id2lat = {}
    id2str = {}
    id2type = {}
    for i in range(len(location['features'])):
        idx = location['features'][i]['properties']['OBJECTID']
        id_list.append(idx)
        id2lon[idx] = []
        id2lat[idx] = []
        id2str[idx] = str(location['features'][i]['geometry']['coordinates'])
        id_type = location['features'][i]['geometry']['type']
        id2type[idx] = id_type
        if id_type == 'Polygon':
            for i1 in range(len(location['features'][i]['geometry']['coordinates'])):
                for i2 in range(len(location['features'][i]['geometry']['coordinates'][i1])):
                    id2lon[idx].append(eval(location['features'][i]['geometry']['coordinates'][i1][i2][0]))
                    id2lat[idx].append(eval(location['features'][i]['geometry']['coordinates'][i1][i2][1]))
        elif id_type == 'MultiPolygon':
            for i1 in range(len(location['features'][i]['geometry']['coordinates'])):
                for i2 in range(len(location['features'][i]['geometry']['coordinates'][i1])):
                    for i3 in range(len(location['features'][i]['geometry']['coordinates'][i1][i2])):
                        id2lon[idx].append(eval(location['features'][i]['geometry']['coordinates'][i1][i2][i3][0]))
                        id2lat[idx].append(eval(location['features'][i]['geometry']['coordinates'][i1][i2][i3][1]))
        else:
            print('error', i)
        id2lon[idx] = sum(id2lon[idx]) / len(id2lon[idx])
        id2lat[idx] = sum(id2lat[idx]) / len(id2lat[idx])

    print('.rel outputing...')
    rel = []
    for i in id2str.keys():
        for j in id2str.keys():
            dist = dis(id2lon[i], id2lat[i], id2lon[j], id2lat[j]) * 1000.0
            rel.append([len(rel), 'geo', i-1, j-1, dist])
    rel = pd.DataFrame(rel, columns=['rel_id', 'type', 'origin_id', 'destination_id', 'cost'])
    rel.to_csv(data_name + '.rel', index=False)

    print('.geo outputing...')
    geo = []
    for i in id2str.keys():
        geo.append([i-1, id2type[i], id2str[i]])
    geo = pd.DataFrame(geo, columns=['geo_id', 'type', 'coordinates'])
    geo.to_csv(data_name + '.geo', index=False)

    # 计算od
    od_data = np.zeros((len(id2str), len(id2str), len(time_dividing_point) - 1))
    print('od calculating...')
    for i in range(dataset_nyc.shape[0]):
        print(str(i) + '/' + str(dataset_nyc.shape[0]))
        time_id = dataset_nyc.iloc[i]['start_time_id']
        start_geo_id = dataset_nyc.iloc[i]['PULocationID'] - 1
        end_geo_id = dataset_nyc.iloc[i]['DOLocationID'] - 1
        od_data[start_geo_id][end_geo_id][time_id] = od_data[start_geo_id][end_geo_id][time_id] + 1

    print('.od outputing...')
    dyna_id = 0
    dyna_file = open(data_name + '.od', 'w')
    dyna_file.write('dyna_id,' + 'type,' + 'time,' + 'origin_id,' + 'destination_id,' + 'flow' + '\n')
    for j in range(od_data.shape[0]):
        for k in range(od_data.shape[1]):
            for i in range(od_data.shape[2]):
                times = convert[i]
                dyna_file.write(str(dyna_id) + ',' + 'state' + ',' + str(times)
                                + ',' + str(j) + ',' + str(k)
                                + ',' + str(od_data[j][k][i]) + '\n')
                dyna_id = dyna_id + 1
                if dyna_id % 10000 == 0:
                    print(str(dyna_id) + '//' + str(od_data.shape[0] * od_data.shape[1] * od_data.shape[2]))
    dyna_file.close()
    print('finish！')
    gen_config(output_dir_flow, file_name, interval)
    end_time = time.time()
    print(end_time - start_time)
