# link: https://www.divvybikes.com/system-data
import json
import math
import os
from datetime import datetime
import time
import numpy as np
import pandas as pd

old_time_format = '%Y-%m-%d %H:%M:%S'
early_old_time_format = '%d/%m/%Y %H:%M:%S'
new_time_format = '%Y-%m-%dT%H:%M:%SZ'
lon_lat_info = {}


def select_lon(x):
    global lon_lat_info
    if x in lon_lat_info.keys():
        return lon_lat_info[x][0]
    return None


def select_lat(x):
    global lon_lat_info
    if x in lon_lat_info.keys():
        return lon_lat_info[x][1]
    return None


def handle_point_geo(df):
    """
    :param df:
    :return: df['s_id', 'poi_name', 'poi_lat', 'poi_lon']
    """
    try:
        start = df[['start_station_id', 'start_station_name',
                    'start_lat', 'start_lng']]
        start.columns = ['s_id', 's_name', 's_lat', 's_lon']
        end = df[['end_station_id', 'end_station_name',
                  'end_lat', 'end_lng']]
        end.columns = ['s_id', 's_name', 's_lat', 's_lon']
    except:
        start = df[['from_station_id', 'from_station_name']]
        start.columns = ['s_id', 's_name']
        start['s_lat'] = start['s_id'].apply(select_lat)
        start['s_lon'] = start['s_id'].apply(select_lon)
        end = df[['to_station_id', 'to_station_name']]
        end.columns = ['s_id', 's_name']
        end['s_lat'] = end['s_id'].apply(select_lat)
        end['s_lon'] = end['s_id'].apply(select_lon)
    station_data = pd.concat((start, end), axis=0)
    station_data = station_data.loc[station_data['s_id'].apply(lambda x: not math.isnan(x))]
    station_data = station_data.loc[station_data['s_lat'].apply(lambda x: x != 0 and x is not None and not math.isnan(x))]
    station_data = station_data.loc[station_data['s_lon'].apply(lambda x: x != 0 and x is not None and not math.isnan(x))]
    station_data = station_data.drop_duplicates()
    station_data.rename(columns={'s_name': 'poi_name',
                                 's_lat': 'poi_lat', 's_lon': 'poi_lon'},
                        inplace=True)
    station_data = station_data[['s_id', 'poi_name', 'poi_lat', 'poi_lon']]
    station_data = station_data.sort_values(by='s_id')
    station_data = station_data.groupby(by='s_id').mean()
    station_data = station_data.reset_index()
    return station_data


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


def partition_to_grid(point_geo, row_num, col_num):
    """
    :param point_geo:
    :param row_num:
    :param col_num:
    :return: df['geo_id', 'poi_name',
    'poi_lat', 'poi_lon', 'row_id', 'column_id']
    """
    # handle row/latitude
    point_geo = point_geo.sort_values(by='poi_lat')
    lat_values = point_geo['poi_lat'].values
    lat_diff = lat_values[-1] - lat_values[0]
    lat_dividing_points = \
        [round(lat_values[0] + lat_diff / row_num * i, 3) for i in range(row_num + 1)]
    point_geo['row_id'] = point_geo.apply(
        lambda x: judge_id(x['poi_lat'], lat_dividing_points),
        axis=1
    )

    # handle col/longitude
    point_geo = point_geo.sort_values(by='poi_lon')
    lon_values = point_geo['poi_lon'].values
    lon_diff = lon_values[-1] - lon_values[0]
    lon_dividing_points = \
        [round(lon_values[0] + lon_diff / col_num * i, 3) for i in range(col_num + 1)]
    point_geo['column_id'] = point_geo.apply(
        lambda x: judge_id(x['poi_lon'], lon_dividing_points),
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

    return point_geo, geo_data


def convert_time(df):
    try:
        df['time'] = df.apply(
            lambda x: pd.to_datetime(
                x['time_str'], format=old_time_format).strftime(new_time_format),
            axis=1)
        df['timestamp'] = df.apply(
            lambda x: float(datetime.timestamp(
                pd.to_datetime(x['time_str'],
                               utc=True,
                               format=old_time_format))),
            axis=1)
    except Exception:
        df['time'] = df.apply(
            lambda x: pd.to_datetime(
                x['time_str'], format=early_old_time_format).strftime(new_time_format),
            axis=1)
        df['timestamp'] = df.apply(
            lambda x: float(datetime.timestamp(
                pd.to_datetime(x['time_str'],
                               utc=True,
                               format=early_old_time_format))),
            axis=1)
    return df


def convert_to_trajectory(df):
    """
    :param df: all data
    :return: df['bikeid', 'geo_id', 'time', 'timestamp']
    """
    global lon_lat_info
    try:
        start = df[['ride_id', 'start_station_id', 'started_at']]
        end = df[['ride_id', 'end_station_id', 'ended_at']]
    except:
        start = df[['bikeid', 'from_station_id', 'start_time']]
        end = df[['bikeid', 'to_station_id', 'end_time']]
    start.columns = ['bikeid', 'geo_id', 'time_str']
    end.columns = ['bikeid', 'geo_id', 'time_str']
    trajectory_data = pd.concat((start, end), axis=0)
    trajectory_data = trajectory_data.loc[trajectory_data['geo_id'].apply(lambda x: not math.isnan(x))]
    trajectory_data = trajectory_data.loc[trajectory_data['geo_id'].apply(lambda x: x in lon_lat_info.keys())]
    trajectory_data = convert_time(trajectory_data)
    return trajectory_data[['bikeid', 'geo_id', 'time', 'timestamp']]


def add_previous_poi(tra_by_bike):
    tra_by_bike = tra_by_bike.sort_values(by='time')
    tra_by_bike['prev_geo_id'] = tra_by_bike['geo_id'].shift(1)
    return tra_by_bike[1:]


def judge_time_id(df, time_dividing_point):
    df['time_id'] = df.apply(
        lambda x: judge_id(x['timestamp'], time_dividing_point),
        axis=1
    )
    return df


def gen_flow_data(trajectory, time_dividing_point):
    """
    :param trajectory:
    :param time_dividing_point:
    :return: ['time', 'row_id', 'column_id', 'inflow', 'outflow']
    """
    trajectory = trajectory.loc[
        (trajectory['row_id'] != trajectory['prev_row_id']) |
        (trajectory['column_id'] != trajectory['prev_column_id'])
        ]

    tra_groups = trajectory.groupby(by='time_id')
    for tra_group in tra_groups:
        tra_group = tra_group[1]
        t = time_dividing_point[tra_group.iloc[0, 11]]
        flow_in = tra_group.groupby(
            by=[
                'row_id',
                'column_id']
        )[['geo_id']].count().sort_index()
        flow_in.columns = ['inflow']
        flow_out = tra_group.groupby(
            by=[
                'prev_row_id',
                'prev_column_id']
        )[['prev_geo_id']].count().sort_index()
        flow_out.index.names = ['row_id', 'column_id']
        flow_out.columns = ['outflow']
        flow = flow_in.join(flow_out, how='outer', on=['row_id', 'column_id'])
        flow = flow.reset_index()
        flow['time'] = timestamp2str(t)

        yield flow


def timestamp2str(timestamp):
    return pd.to_datetime(timestamp, unit='s').strftime(new_time_format)


def fill_empty_flow(flow_data, time_dividing_point, row_num, col_num):
    row_ids = list(range(0, row_num))
    col_ids = list(range(0, col_num))
    time_ids = list(map(timestamp2str, time_dividing_point))

    ids = [(x, y, z) for x in row_ids for y in col_ids for z in time_ids]
    flow_keep = pd.DataFrame(ids, columns=['row_id', 'column_id', 'time'])
    flow_keep = pd.merge(flow_keep, flow_data, how='outer')

    flow_keep = flow_keep.fillna(value={'inflow': 0, 'outflow': 0})
    return flow_keep


def calculate_flow(
        trajectory_data, station_with_id, row_num, col_num, interval):
    station_with_id = station_with_id[['s_id', 'row_id', 'column_id']]
    bike_trajectory = trajectory_data.groupby(by='bikeid')
    bike_trajectory = pd.concat(
        map(lambda x: add_previous_poi(x[1]), bike_trajectory))
    bike_trajectory = bike_trajectory[
        bike_trajectory['geo_id'] != bike_trajectory['prev_geo_id']]

    bike_trajectory = pd.merge(bike_trajectory, station_with_id,
                               left_on='prev_geo_id',
                               right_on='s_id', suffixes=['', '_p'])
    bike_trajectory = bike_trajectory.rename(
        columns={'row_id': 'prev_row_id',
                 'column_id': 'prev_column_id', 's_id': 's_id_p'})
    bike_trajectory = pd.merge(bike_trajectory,
                               station_with_id,
                               left_on='geo_id',
                               right_on='s_id', suffixes=['', '_n'])
    bike_trajectory = bike_trajectory.rename(
        columns={'s_id': 's_id_n'})

    bike_trajectory = bike_trajectory.sort_values(by='timestamp')
    min_timestamp = float(
        math.floor(
            bike_trajectory['timestamp'].values[0] / interval) * interval)
    max_timestamp = float(
        math.ceil(
            bike_trajectory['timestamp'].values[-1] / interval) * interval)
    time_dividing_point = \
        list(np.arange(min_timestamp, max_timestamp, interval))
    bike_trajectory = judge_time_id(bike_trajectory, time_dividing_point)

    flow_data_part = gen_flow_data(bike_trajectory, time_dividing_point)
    flow_data = pd.concat(flow_data_part)
    flow_data = fill_empty_flow(
        flow_data, time_dividing_point, row_num, col_num)
    flow_data['type'] = 'state'
    flow_data = flow_data.reset_index(drop=True)
    flow_data['dyna_id'] = flow_data.index
    flow_data = flow_data[
        ['dyna_id', 'type', 'time', 'row_id', 'column_id', 'inflow', 'outflow']
    ]
    return flow_data


def bike_chi_flow(
        output_dir, output_name, data_set, row_num, col_num, interval=3600):
    data_name = output_dir + "/" + output_name
    # geo data
    station = handle_point_geo(data_set)
    station_with_id, geo_data = partition_to_grid(station, row_num, col_num)
    geo_data.to_csv(data_name + '.geo', index=False)
    print('finish geo')

    # trajectory data
    trajectory_data = convert_to_trajectory(data_set)
    print('finish trajectory')

    # flow data
    flow_data = calculate_flow(
        trajectory_data, station_with_id,
        row_num, col_num, interval=interval)
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
            "time_intervals": interval
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
    print(config)


def generate_lon_lat_info(input_json_url):
    lon_lat_info = {}  # station_id-(lon,lat)
    input_json_file = open(input_json_url, 'r')
    input_json = json.loads(input_json_file.read())
    input_json_file.close()
    input_json = input_json['data']['stations']
    for station_info in input_json:
        station_id = eval(station_info['station_id'])
        lon = station_info['lon']
        lat = station_info['lat']
        lon_lat_info[station_id] = (lon, lat)
    return lon_lat_info


if __name__ == '__main__':
    start_time = time.time()
    # 参数
    # 时间间隔 s
    interval = 3600
    # 开始年月日
    (start_year, start_month, start_day) = (2020, 7, 1)
    # 结束年月日
    (end_year, end_month, end_day) = (2020, 9, 30)
    # 行数
    row_num = 15
    # 列数
    column_num = 18
    # 输入文件夹名称
    input_dir_flow = 'input/BIKECHI'
    # 输出文件名称 与 输出文件夹名称
    file_name = 'BIKECHI%d%02d-%d%02d' \
                % (start_year, start_month, end_year, end_month)
    output_dir_flow = 'output/BIKECHI%d%02d-%d%02d' \
                      % (start_year, start_month, end_year, end_month)
    # 创建输出文件夹
    if not os.path.exists(output_dir_flow):
        os.makedirs(output_dir_flow)

    # 地理位置信息
    input_json_url = input_dir_flow + '/station_information.json'
    lon_lat_info = generate_lon_lat_info(input_json_url)
    # The data files in data_url must have the same format.
    # 待处理的数据文件名
    data_url = [
        input_dir_flow + '/202007-divvy-tripdata.csv',
        input_dir_flow + '/202008-divvy-tripdata.csv',
        input_dir_flow + '/202009-divvy-tripdata.csv'
    ]
    data_url = tuple(data_url)
    dataset_chi = pd.concat(
        map(lambda x: pd.read_csv(x), data_url), axis=0
    )
    dataset_chi.reset_index(drop=True, inplace=True)
    print('finish read csv')

    bike_chi_flow(
        output_dir_flow,
        file_name,
        dataset_chi,
        row_num,
        column_num,
        interval=interval
    )
    gen_config(output_dir_flow, file_name, row_num, column_num, interval)
    print('finish')
    end_time = time.time()
    print(end_time - start_time)
