import json
import pandas as pd
import os
from datetime import datetime
import numpy as np
import time

# total number of points:15 million
# the total distance of the trajectories: 9 million km
# average sampling interval: 177 seconds with a distance of about 623 meters.
# taxi_id, date_time, longitude, latitude
# 北京左上角(39.83°, 116.25°)，右下角(40.12°, 116.64°)，划分成了32 * 32的网格。
TAXI_NUM = 10357
LAT_MIN = 39.83
LAT_MAX = 40.12
LON_MIN = 116.25
LON_MAX = 116.64
old_time_format = '%Y-%m-%d %H:%M:%S'
new_time_format = '%Y-%m-%dT%H:%M:%SZ'
MIN_TIME = '2008-02-02 00:00:00'
MAX_TIME = '2008-02-09 00:00:00'
MIN_TIMESTAMP = float(
    datetime.timestamp(
        pd.to_datetime(MIN_TIME, utc=True, format=old_time_format)))
MAX_TIMESTAMP = float(
    datetime.timestamp(
        pd.to_datetime(MAX_TIME, utc=True, format=old_time_format)))


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
    :param data_set: ['taxi_id', 'date_time', 'longitude', 'latitude']
    :param row_num: # of rows
    :param col_num: # of columns
    :return: data_set_with_rc_id:
                        ['taxi id', 'date_time', 'row_id', 'column_id']
    :return: geo_data['geo_id', 'type', 'coordinates', 'row_id', 'column_id']
    """
    # handle row/latitude
    lat_diff = LAT_MAX - LAT_MIN
    lat_dividing_points = \
        [round(LAT_MIN + lat_diff / row_num * i, 3)
         for i in range(row_num + 1)]
    # print(len(lat_dividing_points))
    data_set['row_id'] = data_set.apply(
        lambda x: judge_id(x['latitude'], lat_dividing_points),
        axis=1
    )

    # handle col/longitude
    lon_diff = LON_MAX - LON_MIN
    lon_dividing_points = \
        [round(LON_MIN + lon_diff / col_num * i, 3)
         for i in range(col_num + 1)]
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

    return data_set[['taxi_id', 'date_time', 'row_id', 'column_id']], geo_data


def convert_time(df):
    df['time'] = df.apply(
        lambda x: x['date_time'].replace(' ', 'T') + 'Z',
        axis=1)
    df['timestamp'] = df.apply(
        lambda x: float(datetime.timestamp(
            pd.to_datetime(x['date_time'],
                           utc=True,
                           format=old_time_format))),
        axis=1)
    return df


def convert_to_trajectory(df):
    """
    :param df: ['taxi id', 'date time', 'row_id', 'column_id']
    :return: df: ['taxi id', 'time', 'row_id', 'column_id', 'timestamp']
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


def t_drive_flow(
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
    # trajectory_data.to_csv('trajectory_data.geo', index=False)
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
    config = json.dumps(data)
    with open(output_dir_flow + "/config.json", "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)
    # print(config)


if __name__ == '__main__':
    start_time = time.time()
    # 参数
    # 测试时选取的taxi数量
    test_taxi_num = TAXI_NUM
    # test_taxi_num = 500
    # 时间间隔
    interval = 3600
    # 开始年月
    (start_year, start_month, start_day) = (2008, 2, 2)
    # 结束年月
    (end_year, end_month, end_day) = (2008, 2, 8)
    # 行数
    row_num = 32
    # 列数
    column_num = 32
    # 输出文件名称
    file_name = 'T-drive-%d' % test_taxi_num
    # 输出文件夹名称
    output_dir_flow = 'output/T-drive-%d' % test_taxi_num
    # 输入文件夹名称
    input_dir_flow = 'input/T-drive'

    data_url = [
        input_dir_flow + '/' + str(i + 1) + '.txt'
        for i in range(test_taxi_num)]
    print(data_url)

    # 创建输出文件夹
    if not os.path.exists(output_dir_flow):
        os.makedirs(output_dir_flow)

    # 对空文件进行过滤
    data_urls = []
    for i in data_url:
        if os.path.getsize(i) != 0:
            data_urls.append(i)
    # 读入文件并实现拼接
    data_set_t = pd.concat(
        map(lambda x: pd.read_csv(x, header=None), data_urls), axis=0
    )  # 纵向拼接数据
    data_set_t.reset_index(drop=True, inplace=True)
    data_set_t.columns = ['taxi_id', 'date_time', 'longitude', 'latitude']

    # 过滤超出北京范围的数据
    data_set_t = data_set_t.loc[
        data_set_t['longitude'].apply(lambda x:(LON_MIN <= x <= LON_MAX))]
    data_set_t = data_set_t.loc[
        data_set_t['latitude'].apply(lambda x: (LAT_MIN <= x <= LAT_MAX))]

    print('finish read csv')
    # data_set_t.to_csv('concat.csv', line_terminator='\n')

    # 调用处理函数，生成.grid 和.geo文件
    t_drive_flow(
        output_dir_flow,
        file_name,
        data_set_t,
        row_num,
        column_num,
        interval=interval
    )
    # 生成config.json文件
    gen_config(output_dir_flow, file_name, row_num, column_num, interval)
    print('finish config')
    end_time = time.time()
    print(end_time - start_time)
