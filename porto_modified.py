import json
import pandas as pd
import os
import numpy as np
import time
import math

old_time_format = '%Y-%m-%d %H:%M:%S'
new_time_format = '%Y-%m-%dT%H:%M:%SZ'

# MIN_TIME = '2013-07-01 00:00:00'
# MAX_TIME = '2014-07-01 00:00:00'
# MIN_TIMESTAMP = float(
#     datetime.timestamp(
#         pd.to_datetime(MIN_TIME, utc=True, format=old_time_format)))
# MAX_TIMESTAMP = float(
#     datetime.timestamp(
#         pd.to_datetime(MAX_TIME, utc=True, format=old_time_format)))


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
    geo = {"including_types": [
        "Polygon"
    ],
        "Polygon": {
            "row_id": "num",
            "column_id": "num"
        }
    }
    return geo


def gen_config_grid(row_num, column_num):
    grid = {
        "including_types": [
            "state"
        ],
        "state": {
            "row_id": row_num,
            "column_id": column_num,
            "inflow": "num",
            "outflow": "num"
        }
    }
    return grid


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
            "time_intervals": interval,
        }
    return info


def gen_config(output_dir_flow, file_name, row_num, column_num, interval):
    print('begin config')
    config = {}
    data = json.loads(json.dumps(config))
    data["geo"] = gen_config_geo()
    data["grid"] = gen_config_grid(row_num, column_num)
    data["info"] = gen_config_info(file_name, interval)
    # config = json.dumps(data)
    with open(output_dir_flow + "/config.json", "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)
    # print(config)
    print('finish config')


if __name__ == '__main__':
    start_time = time.time()
    # 参数
    # 时间间隔
    interval = 3600
    # 行数
    row_num = 20
    # 列数
    column_num = 10
    # 开始年月日
    (start_year, start_month, start_day) = (2013, 7, 1)
    # 结束年月日
    (end_year, end_month, end_day) = (2013, 9, 30)
    # 输入文件名称
    input_file_name = 'Porto.csv'
    # input_file_name = 'train.csv'
    # 输出文件名称
    file_name = input_file_name.split('.')[0]
    # 输出文件夹名称
    output_dir_flow = 'output/' + file_name + '/'
    # 输入文件夹名称
    input_dir_flow = 'input/Porto/'

    data_url = input_dir_flow + input_file_name

    # 创建输出文件夹
    if not os.path.exists(output_dir_flow):
        os.makedirs(output_dir_flow)

    # 读入文件
    data_set_Porto = pd.read_csv(
        data_url,
        usecols=[4, 5, 8],
        dtype={'TAXI_ID': np.int64, 'TIMESTAMP': np.int64}
    )  #
    data_set_Porto.reset_index(drop=True, inplace=True)
    data_set_Porto.columns = ['taxi_id', 'timestamp', 'polyline']
    print('finish reading csv')
    print(data_set_Porto.dtypes)
    print('shape:', data_set_Porto.shape)

    # 筛选时间范围（由6个参数决定）
    dt1 = '%d-%02d-%02dT00:00:00Z' % (start_year, start_month, start_day)
    dt2 = '%d-%02d-%02dT23:59:59Z' % (end_year, end_month, end_day)
    # print(dt1, dt2)
    stimes = time.mktime(time.strptime(dt1, '%Y-%m-%dT%H:%M:%SZ'))
    etimes = time.mktime(time.strptime(dt2, '%Y-%m-%dT%H:%M:%SZ'))
    # print(stimes, etimes)
    data_set_Porto = data_set_Porto[(data_set_Porto['timestamp'] <= etimes)
                                    & (data_set_Porto['timestamp'] >= stimes)]
    print('select time, shape:', data_set_Porto.shape)
    # # 去掉有空值列
    # data_set_Porto.dropna(subset=['taxi_id', 'timestamp', 'polyline'], inplace=True)
    # print('drop na, shape:', data_set_Porto.shape)

    # 生成时间序列
    print('begin to generate dividing points')
    # 时间戳的最小最大值，按interval对齐
    min_timestamp = stimes
    max_timestamp = etimes
    min_timestamp = float(math.floor(min_timestamp / interval) * interval)
    max_timestamp = float(math.ceil(max_timestamp / interval) * interval)
    # 以interval为颗粒度生成时间分割序列
    time_dividing_point = \
        list(np.arange(min_timestamp, max_timestamp, interval))
    # print(time_dividing_point)
    convert = []
    for t in time_dividing_point:
        convert.append(time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(t)))

    # 生成经纬度范围
    # 遍历polyline，维护经纬度的最大最小值
    LAT_MAX, LAT_MIN = float("-inf"), float("inf")
    LON_MAX, LON_MIN = float("-inf"), float("inf")
    for index, row in data_set_Porto.iterrows():
        poly_lst = eval(row['polyline'])
        for lon, lat in poly_lst:
            LON_MAX = max(LON_MAX, lon)
            LON_MIN = min(LON_MIN, lon)
            LAT_MAX = max(LAT_MAX, lat)
            LAT_MIN = min(LAT_MIN, lat)

    # 生成经纬度列表
    lat_diff = LAT_MAX - LAT_MIN
    lat_dividing_points = \
        [round(LAT_MIN + lat_diff / row_num * i, 3)
         for i in range(row_num + 1)]
    lon_diff = LON_MAX - LON_MIN
    lon_dividing_points = \
        [round(LON_MIN + lon_diff / column_num * i, 3)
         for i in range(column_num + 1)]

    # 生成 grid
    # generate gird data (.geo)
    print('begin to generate geo')
    geo_data = pd.DataFrame(
        columns=['geo_id', 'type', 'coordinates', 'row_id', 'column_id'])
    for i in range(row_num):
        for j in range(column_num):
            index = i * column_num + j
            coordinates = [[
                [lon_dividing_points[j], lat_dividing_points[i]],
                [lon_dividing_points[j + 1], lat_dividing_points[i]],
                [lon_dividing_points[j + 1], lat_dividing_points[i + 1]],
                [lon_dividing_points[j], lat_dividing_points[i + 1]],
                [lon_dividing_points[j], lat_dividing_points[i]]
            ]]  # list of list of [lon, lat]
            geo_data.loc[index] = [index, 'Polygon', coordinates, i, j]
    geo_data.to_csv(output_dir_flow + file_name + '.geo', index=False)
    print('finish geo')

    print('size of grid data:', len(time_dividing_point), len(lon_dividing_points) - 1, len(lat_dividing_points) - 1, 2)

    # 存储结果的数据结构，最后一维代表inflow 和 outflow
    grid_data = np.zeros((len(time_dividing_point), len(lon_dividing_points) - 1, len(lat_dividing_points) - 1, 2))
    print("begin to calculate grid")
    # 逐行统计in/out_flow
    for index, row in data_set_Porto.iterrows():
        # 初始化为第一个轨迹点的数据
        trajectory_list = eval(row['polyline'])
        if len(trajectory_list) == 0:
            continue
        timestamp = row['timestamp']
        time_index = judge_id(timestamp, time_dividing_point)
        trajectory0 = trajectory_list[0]
        lon_index = judge_id(trajectory0[0], lon_dividing_points)
        lat_index = judge_id(trajectory0[1], lat_dividing_points)
        # 对于一辆taxi的轨迹数据
        for lon, lat in trajectory_list[1:]:
            timestamp += 15 * 1000  # 15s
            if timestamp > etimes:
                break
            time_index_new = judge_id(timestamp, time_dividing_point)
            lon_index_new = judge_id(lon, lon_dividing_points)
            lat_index_new = judge_id(lat, lat_dividing_points)
            if lon_index == lon_index_new and lat_index == lat_index_new:
                time_index = time_index_new
            else:
                # out += 1
                grid_data[time_index][lon_index][lat_index][1] = grid_data[time_index][lon_index][lat_index][1] + 1
                # in += 1
                grid_data[time_index_new][lon_index_new][lat_index_new][0] =\
                    grid_data[time_index_new][lon_index_new][lat_index_new][0] + 1
                time_index = time_index_new
                lon_index = lon_index_new
                lat_index = lat_index_new

    # grid_data.to_csv('flow_data.csv')
    print('finish calculating grid')

    print('begin to write .grid')
    grid_file = open(output_dir_flow + file_name + '.grid', 'w')
    # 'dyna_id', 'type', 'time', 'row_id', 'column_id', 'inflow', 'outflow'
    grid_file.write('dyna_id,' + 'type,' + 'time,' + 'row_id,' + 'column_id,' + 'inflow,' + 'outflow' + '\n')
    dyna_id = 0
    for lat_index in range(grid_data.shape[2]):
        for lon_index in range(grid_data.shape[1]):
            for time_index in range(grid_data.shape[0]):
                times = convert[time_index]
                grid_file.write(str(dyna_id) + ',' + 'state' + ',' + str(times)
                                + ',' + str(lat_index) + ',' + str(lon_index)
                                + ',' + str(grid_data[time_index][lon_index][lat_index][0]) +
                                ',' + str(grid_data[time_index][lon_index][lat_index][1]) + '\n')
                dyna_id = dyna_id + 1
                if dyna_id % 10000 == 0:
                    print(str(dyna_id) + '//' + str(grid_data.shape[0] * grid_data.shape[1] * grid_data.shape[2]))
    grid_file.close()
    print('finish grid file')

    # 生成config.json文件
    gen_config(output_dir_flow, file_name, row_num, column_num, interval)
    print('finish')
    end_time = time.time()
    print(end_time - start_time)
