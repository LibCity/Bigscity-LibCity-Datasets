import json
import pandas as pd
import os
from datetime import datetime
import numpy as np
import time

old_time_format = '%Y-%m-%d %H:%M:%S'
new_time_format = '%Y-%m-%dT%H:%M:%SZ'
MIN_TIME = '2013-07-01 00:00:00'
MAX_TIME = '2014-07-01 00:00:00'
MIN_TIMESTAMP = float(
    datetime.timestamp(
        pd.to_datetime(MIN_TIME, utc=True, format=old_time_format)))
MAX_TIMESTAMP = float(
    datetime.timestamp(
        pd.to_datetime(MAX_TIME, utc=True, format=old_time_format)))


def split_polyline(data):
    # step 1: delete prefix [[ and postfix ]] in data['polyline']
    data['polyline'] = data['polyline'].apply(lambda x: x[2:-2])

    # step 2: horizontally divide polyline into many lines,each line
    #           contains single point
    data_split = data.drop('polyline', axis=1).join(
        data['polyline'].str.split(
            r'\],\[', expand=True
        ).stack().reset_index(
            level=1, drop=True
        ).rename('poly'))
    data_split.reset_index(drop=True, inplace=True)
    data_split['prev_taxi_id'] = data_split['taxi_id'].shift(1)
    data_split.loc[0, 'prev_taxi_id'] = -1
    data_split['prev_taxi_id'] = data_split['prev_taxi_id'].astype('int')
    time_acc = 0
    for index, line in data_split.iterrows():
        if line['taxi_id'] == line['prev_taxi_id']:
            time_acc += 15 * 1000
            data_split.loc[index, 'timestamp'] += time_acc
        else:
            time_acc = 0

    # step 3: vertically divide poly into longitude and latitude
    data_split[['longitude', 'latitude']] =\
        data_split['poly'].str.split(',', n=1, expand=True)
    data_split.drop('poly', axis=1, inplace=True)
    data_split['longitude'] = data_split['longitude'].str.strip()
    data_split['latitude'] = data_split['latitude'].str.strip()

    # step 4: drop lines that contains null data
    data_split.dropna(subset=['longitude', 'latitude'], inplace=True)

    # step 5: change data type of lat and lon from str to double
    data_split['longitude'] = data_split['longitude'].astype('float')
    data_split['latitude'] = data_split['latitude'].astype('float')
    return data_split


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


def partition_to_grid(data_set, row_num, col_num):
    """
    :param data_set: ['taxi_id', 'timestamp', 'longitude', 'latitude']
    :param row_num: # of rows
    :param col_num: # of columns
    :return: data_set_with_rc_id:
                        ['taxi_id', 'timestamp', 'row_id', 'column_id']
    :return: geo_data['geo_id', 'type', 'coordinates', 'row_id', 'column_id']
    """
    # handle row/latitude
    data_set = data_set.sort_values(by='latitude')
    lat_values = data_set['latitude'].values
    LAT_MAX = lat_values[-1]
    LAT_MIN = lat_values[0]
    lat_diff = LAT_MAX - LAT_MIN
    lat_dividing_points = \
        [round(LAT_MIN + lat_diff / row_num * i, 3)
         for i in range(row_num + 1)]
    # print(lat_dividing_points)
    data_set['row_id'] = data_set.apply(
        lambda x: judge_id(x['latitude'], lat_dividing_points),
        axis=1
    )

    # handle col/longitude
    data_set = data_set.sort_values(by='longitude')
    lon_values = data_set['longitude'].values
    LON_MAX = lon_values[-1]
    LON_MIN = lon_values[0]
    lon_diff = LON_MAX - LON_MIN
    lon_dividing_points = \
        [round(LON_MIN + lon_diff / col_num * i, 3)
         for i in range(col_num + 1)]
    # print(lon_dividing_points)
    data_set['column_id'] = data_set.apply(
        lambda x: judge_id(x['longitude'], lon_dividing_points),
        axis=1
    )

    # generate gird data (.geo)
    geo_data = pd.DataFrame(
        columns=['geo_id', 'type', 'coordinates', 'row_id', 'column_id'])
    for i in range(row_num):
        for j in range(col_num):
            index = i * col_num + j
            coordinates = [[
                [lon_dividing_points[j], lat_dividing_points[i]],
                [lon_dividing_points[j + 1], lat_dividing_points[i]],
                [lon_dividing_points[j + 1], lat_dividing_points[i + 1]],
                [lon_dividing_points[j], lat_dividing_points[i + 1]],
                [lon_dividing_points[j], lat_dividing_points[i]]
            ]]  # list of list of [lon, lat]
            geo_data.loc[index] = [index, 'Polygon', coordinates, i, j]

    return data_set[['taxi_id', 'timestamp', 'row_id', 'column_id']], geo_data


def convert_time(df):
    df['time'] = df.apply(
        lambda x:
        datetime.fromtimestamp(x['timestamp']).strftime(new_time_format),
        axis=1)
    return df


def convert_to_trajectory(df):
    """
    :param df: ['taxi_id', 'date_time', 'row_id', 'column_id']
    :return: df: ['taxi_id', 'time', 'row_id', 'column_id', 'timestamp']
    """
    trajectory_data = convert_time(df)
    return trajectory_data[
        ['taxi_id', 'time', 'row_id', 'column_id', 'timestamp']]


def add_previous_rc_id(tra_by_bike):
    tra_by_bike = tra_by_bike.sort_values(by='time')
    # tra_by_bike['prev_row_id'].astype("int")
    # tra_by_bike['prev_column_id'].astype("int")
    tra_by_bike['prev_row_id'] = tra_by_bike['row_id'].shift(1)
    tra_by_bike['prev_column_id'] = tra_by_bike['column_id'].shift(1)
    return tra_by_bike[1:]


def judge_time_id(df, time_dividing_point):
    df['time_id'] = df.apply(
        lambda x: judge_id(x['timestamp'], time_dividing_point),
        axis=1
    )
    return df


def gen_flow_data1(trajectory, time_dividing_point):
    """
    :param trajectory:
    :param time_dividing_point:
    :return: ['time', 'row_id', 'column_id', 'inflow', 'outflow']
    """
    trajectory = trajectory[
        (trajectory.prev_row_id != trajectory.row_id) |
        (trajectory.prev_column_id != trajectory.column_id)]
    tra_groups = trajectory.groupby(by='time_id')
    for tra_group in tra_groups:
        tra_group = tra_group[1]
        # print(tra_group)
        t = time_dividing_point[tra_group.iloc[0].loc['time_id']]
        flow_in = tra_group.groupby(
            by=[
                'row_id',
                'column_id']
        )[['taxi_id']].count().sort_index()
        flow_in.columns = ['inflow']
        flow_out = tra_group.groupby(
            by=[
                'prev_row_id',
                'prev_column_id']
        )[['taxi_id']].count().sort_index()
        flow_out.index.names = ['row_id', 'column_id']
        flow_out.columns = ['outflow']
        flow = flow_in.join(flow_out, how='outer', on=['row_id', 'column_id'])
        flow = flow.reset_index()
        # flow['time'] = util.timestamp_to_str(t)
        # print(t)
        flow['time'] = timestamp2str(t)
        # print(timestamp2str(t))
        yield flow


def timestamp2str(timestamp):
    return pd.to_datetime(timestamp, unit='s').strftime(new_time_format)


def fill_empty_flow(flow_data, time_dividing_point, row_num, col_num):
    # 主要通过生成一个全数据的data frame 与flow_data合并实现
    row_ids = list(range(0, row_num))
    col_ids = list(range(0, col_num))
    time_ids = list(map(timestamp2str, time_dividing_point))

    ids = [(x, y, z) for x in row_ids for y in col_ids for z in time_ids]
    flow_keep = pd.DataFrame(ids, columns=['row_id', 'column_id', 'time'])
    flow_keep = pd.merge(flow_keep, flow_data, how='outer')

    flow_keep = flow_keep.fillna(value={'inflow': 0, 'outflow': 0})
    return flow_keep


def calculate_flow(
        trajectory_data, row_num, col_num, interval):
    """
    :param trajectory_data:
                ['taxi_id', 'time', 'row_id', 'column_id', 'timestamp']
    :param row_num
    :param col_num
    :param interval
    :return: ['time', 'row_id', 'column_id', 'inflow', 'outflow']
    """
    # 对taxi_id进行group
    taxi_trajectory = trajectory_data.groupby(by='taxi_id')
    # print(taxi_trajectory)

    ########################################
    # 对taxi_trajectory添加上一个地点的区域：prev_col_id,prev_row_id
    taxi_trajectory = pd.concat(
        map(lambda x: add_previous_rc_id(x[1]), taxi_trajectory))

    # 对新生成列的类型进行转换
    taxi_trajectory['prev_row_id'] = \
        taxi_trajectory['prev_row_id'].astype("int64")
    taxi_trajectory['prev_column_id'] = \
        taxi_trajectory['prev_column_id'].astype("int64")

    # 若起点和终点位于一块，则drop这一行
    taxi_trajectory = taxi_trajectory[
        ~((taxi_trajectory['row_id'] == taxi_trajectory['prev_row_id']) & (
                taxi_trajectory['column_id'] ==
                taxi_trajectory['prev_column_id']))]

    # 时间戳的最小最大值，以interval为颗粒度。
    min_timestamp = MIN_TIMESTAMP
    max_timestamp = MAX_TIMESTAMP
    time_dividing_point = \
        list(np.arange(min_timestamp, max_timestamp, interval))
    # print(time_dividing_point)
    # 为taxi_trajectory加上time_id
    taxi_trajectory = judge_time_id(taxi_trajectory, time_dividing_point)
    # taxi_trajectory.to_csv('with_time.csv')

    # 接下来需要根据taxi_trajectory和time_dividing_point数组统计出入流量
    flow_data_part = gen_flow_data1(taxi_trajectory, time_dividing_point)
    # print("type of data part:" + str(type(flow_data_part)))
    flow_data = pd.concat(flow_data_part)
    # ,row_id,column_id,inflow,outflow,time
    # flow_data.to_csv('flow1.csv')

    flow_data = fill_empty_flow(
        flow_data, time_dividing_point, row_num, col_num)
    # flow_data.to_csv('output/NYC_BIKE_flow_test/NYC_BIKE_flow_fill_empty.csv')
    flow_data['type'] = 'state'
    flow_data = flow_data.reset_index(drop=True)
    flow_data['dyna_id'] = flow_data.index
    flow_data = flow_data[
        ['dyna_id', 'type', 'time', 'row_id', 'column_id', 'inflow', 'outflow']
    ]
    return flow_data


def Porto_flow(
        output_dir, output_name, data_set, row_num, col_num, interval=3600):
    data_name = output_dir + "/" + output_name

    # 1. calculate row_id and column_id for data_set and
    #   generate geo_data(.geo)
    data_set_with_rc_id, geo_data = \
        partition_to_grid(data_set, row_num, col_num)
    geo_data.to_csv(data_name + '.geo', index=False)
    print('finish geo')

    # trajectory data, add timestamp
    trajectory_data = convert_to_trajectory(data_set_with_rc_id)
    trajectory_data.to_csv('trajectory_data.csv', index=False)
    print('finish trajectory')

    # flow data
    flow_data = calculate_flow(
        trajectory_data,
        row_num,
        col_num,
        interval=interval
    )
    flow_data.to_csv(data_name + '.grid', index=False)
    print('finish flow')


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
    # 开始年月
    (start_year, start_month, start_day) = (2013, 7, 1)
    # 结束年月
    (end_year, end_month, end_day) = (2013, 9, 30)
    # 输入文件名称
    # input_file_name = 'Porto_1000.csv'
    input_file_name = 'train.csv'
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
    data_set_Porto = pd.read_csv(data_url, usecols=[4, 5, 8])
    data_set_Porto.reset_index(drop=True, inplace=True)
    data_set_Porto.columns = ['taxi_id', 'timestamp', 'polyline']
    print(data_set_Porto.shape)

    dt1 = '%d-%02d-%02dT00:00:00Z' % (start_year, start_month, start_day)
    dt2 = '%d-%02d-%02dT23:59:59Z' % (end_year, end_month, end_day)
    stimes = time.mktime(time.strptime(dt1, '%Y-%m-%dT%H:%M:%SZ'))
    etimes = time.mktime(time.strptime(dt2, '%Y-%m-%dT%H:%M:%SZ'))
    data_set_Porto = data_set_Porto[(data_set_Porto['timestamp'] <= etimes)
                                    & (data_set_Porto['timestamp'] >= stimes)]
    print(data_set_Porto.shape)
    print('finish read csv')

    # 对输入文件进行预处理，分割出polyline的数据
    data_set_Porto = split_polyline(data_set_Porto)
    print('finish split polyline')

    # 调用处理函数，生成.grid 和.geo文件
    Porto_flow(
        output_dir_flow,
        file_name,
        data_set_Porto,
        row_num,
        column_num,
        interval=interval
    )
    # 生成config.json文件
    gen_config(output_dir_flow, file_name, row_num, column_num, interval)
    print('finish')
    end_time = time.time()
    print(end_time - start_time)
