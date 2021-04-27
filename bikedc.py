import numpy as np
import pandas as pd
import os
import json
import csv
import math
from datetime import datetime
import time
old_time_format = '%Y-%m-%d %H:%M:%S'
new_time_format = '%Y-%m-%dT%H:%M:%SZ'


def get_data_url_year(input_dir_flow, start_year, start_month, end_year, end_month):
    res = []
    pattern_year = input_dir_flow + "/%d-capitalbikeshare-tripdata.csv"
    i = start_year
    while i <= end_year:
        res.append(pattern_year % i)
        i += 1
    return res


def get_data_url_Q(input_dir_flow, start_year, start_month, end_year, end_month):
    res = []
    pattern_Q = input_dir_flow + "/%dQ%d-capitalbikeshare-tripdata.csv"

    start_Q = (start_month - 1) // 3 + 1
    end_Q = (end_month - 1) // 3 + 1
    year = start_year
    while year <= end_year:
        if year == start_year:
            Q = start_Q
        else:
            Q = 1
        if year == end_year:
            end = end_Q
        else:
            end = 4
        while Q <= end:
            res.append(pattern_Q % (year, Q))
            Q += 1
        year += 1
    return res


def get_data_url_month(input_dir_flow, start_year, start_month, end_year, end_month):
    res = []
    pattern_month = input_dir_flow + "/%d%02d-capitalbikeshare-tripdata.csv"
    pattern_201801 = input_dir_flow + "/%d%02d_capitalbikeshare_tripdata.csv"
    year = start_year
    while year <= end_year:
        if year == start_year:
            month = start_month
        else:
            month = 1
        if year == end_year:
            end = end_month
        else:
            end = 12
        while month <= end:
            if year == 2018 and month == 1:
                res.append(pattern_201801 % (year, month))
            else:
                res.append(pattern_month % (year, month))
            month += 1
        year += 1
    return res


def get_data_url(input_dir_flow, start_year, start_month, end_year, end_month):
    res = []
    start_str = '%d-%02d' % (start_year, start_month)
    end_str = '%d-%02d' % (end_year, end_month)
    if start_str <= '2011-12':
        if end_str <= '2011-12':
            res += get_data_url_year(
                input_dir_flow,
                start_year,
                start_month,
                end_year,
                end_month
            )
            return res
        else:
            res += get_data_url_year(
                input_dir_flow,
                start_year,
                start_month,
                2011,
                12
            )
            start_year, start_month = 2021, 1
            start_str = '%d-%02d' % (start_year, start_month)

    if start_str <= '2017-12':
        if end_str <= '2017-12':
            res += get_data_url_Q(
                input_dir_flow,
                start_year,
                start_month,
                end_year,
                end_month
            )
            return res
        else:
            res += get_data_url_Q(
                input_dir_flow,
                start_year,
                start_month,
                2017,
                12
            )
            start_year, start_month = 2018, 1

    res += get_data_url_month(
        input_dir_flow,
        start_year,
        start_month,
        end_year,
        end_month
    )
    return res


def gen_station_csv():
    json_file_path = 'station_information/station_information.json'
    csv_file_path = 'station_information/station_information.csv'

    # 获得station_data
    f = open(json_file_path, "r")
    json_data = json.load(f)['data']
    station_data = json_data['stations']

    # 写入相关数据至csv文件
    with open(csv_file_path, "w", newline="") as fw:
        writer = csv.writer(fw)

        # 先写入columns_name, 写入多行用writerows
        writer.writerow(["name", "lon", "lat"])
        for station in station_data:
            lat = station['lat']
            lon = station['lon']
            name = station['name']
            writer.writerow([name, lon, lat])


def add_station_loc(data_set):
    # 若含有起止站点坐标，选取相关列并改列名
    if 'start_lat' in data_set.columns.values:
        data_set = data_set[[
            'started_at',
            'ended_at',
            'start_station_name',
            'start_station_id',
            'end_station_name',
            'end_station_id',
            'start_lat',
            'start_lng',
            'end_lat',
            'end_lng',
            'ride_id',
        ]]
        data_set = data_set.rename(columns={
            'started_at': 'starttime',
            'ended_at': 'stoptime',
            'start_station_name': 'start station name',
            'start_station_id': 'start station id',
            'end_station_name': 'end station name',
            'end_station_id': 'end station id',
            'start_lat': 'start station latitude',
            'start_lng': 'start station longitude',
            'end_lat': 'end station latitude',
            'end_lng': 'end station longitude',
            'ride_id': 'bikeid',
        })
        data_set['bikeid'] = [i for i in range(data_set.shape[0])]
        return data_set

    # 若不含有起止站点坐标，则首先检查是否已生成station_information.csv
    if not os.path.exists('station_information/station_information.csv'):
        gen_station_csv()

    # 确保已生成station_information.csv后，进行拼接操作
    station_information = \
        pd.read_csv('station_information/station_information.csv')
    data_set = pd.merge(data_set, station_information,
                        left_on='Start station',
                        right_on='name')
    data_set = data_set.rename(columns={
        'Start date': 'starttime',
        'End date': 'stoptime',
        'Start station': 'start station name',
        'Start station number': 'start station id',
        'End station': 'end station name',
        'End station number': 'end station id',
        'lat': 'start station latitude',
        'lon': 'start station longitude',
        'Bike number': 'bikeid',
    })
    data_set = pd.merge(data_set, station_information,
                        left_on='end station name',
                        right_on='name')
    data_set = data_set.rename(columns={
        'lat': 'end station latitude',
        'lon': 'end station longitude',
    })
    return data_set[['starttime',
                     'stoptime',
                     'start station name',
                     'start station id',
                     'end station name',
                     'end station id',
                     'start station latitude',
                     'start station longitude',
                     'end station latitude',
                     'end station longitude',
                     'bikeid',
                     ]]


def handle_point_geo(df):
    """
    :param df:
    :return: df['geo_id', 'poi_name', 'poi_lat', 'poi_lon']
    """
    # 选出与start相关的列
    start = df[['start station id', 'start station name',
                'start station latitude', 'start station longitude']]
    # 重命名与start相关的列
    start.columns = ['s_id', 's_name', 's_lat', 's_lon']
    # 选出与end相关的列
    end = df[['end station id', 'end station name',
              'end station latitude', 'end station longitude']]
    # 重命名与end相关的列
    end.columns = ['s_id', 's_name', 's_lat', 's_lon']
    # 将开始和结束数据纵向拼接
    station_data = pd.concat((start, end), axis=0)
    # 去除冗余数据
    station_data = station_data.drop_duplicates()
    # 重命名geo数据
    station_data.rename(columns={'s_name': 'poi_name',
                                 's_lat': 'poi_lat', 's_lon': 'poi_lon'},
                        inplace=True)

    station_data = station_data.loc[
        station_data['poi_lat'].apply(
            lambda x: x != 0 and x is not None and not math.isnan(x))]
    station_data = station_data.loc[
        station_data['poi_lon'].apply(
            lambda x: x != 0 and x is not None and not math.isnan(x))]
    # 确认只存在这些列
    station_data = station_data[['s_id', 'poi_name', 'poi_lat', 'poi_lon']]
    # 排序
    station_data = station_data.sort_values(by='s_id')
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
        [round(lat_values[0] + lat_diff / row_num * i, 3)
         for i in range(row_num + 1)]
    # print(len(lat_dividing_points))
    point_geo['row_id'] = point_geo.apply(
        lambda x: judge_id(x['poi_lat'], lat_dividing_points),
        axis=1
    )

    # handle col/longitude
    point_geo = point_geo.sort_values(by='poi_lon')
    lon_values = point_geo['poi_lon'].values
    lon_diff = lon_values[-1] - lon_values[0]
    lon_dividing_points = \
        [round(lon_values[0] + lon_diff / col_num * i, 3)
         for i in range(col_num + 1)]
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
    df['time'] = df.apply(
        lambda x: x['time_str'].replace(' ', 'T') + 'Z',
        axis=1)
    df['timestamp'] = df.apply(
        lambda x: float(datetime.timestamp(
            pd.to_datetime(x['time_str'],
                           utc=True,
                           format=old_time_format))),
        axis=1)
    return df


def convert_to_trajectory(df):
    """
    :param df: all data
    :return: df['bikeid', 'geo_id', 'time', 'timestamp']
    """
    start = df[['bikeid', 'start station id', 'starttime']]
    start.columns = ['bikeid', 'geo_id', 'time_str']
    end = df[['bikeid', 'end station id', 'stoptime']]
    end.columns = ['bikeid', 'geo_id', 'time_str']
    trajectory_data = pd.concat((start, end), axis=0)
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
    # print(time_ids)
    ids = [(x, y, z) for x in row_ids for y in col_ids for z in time_ids]
    flow_keep = pd.DataFrame(ids, columns=['row_id', 'column_id', 'time'])
    flow_keep = pd.merge(flow_keep, flow_data, how='outer')

    flow_keep = flow_keep.fillna(value={'inflow': 0, 'outflow': 0})
    return flow_keep


def calculate_flow(
        trajectory_data, station_with_id, row_num, col_num, interval):
    # 对station_with_id选取相关列
    station_with_id = station_with_id[['s_id', 'row_id', 'column_id']]
    station_with_id = station_with_id.drop_duplicates()
    # 对bike id进行group
    bike_trajectory = trajectory_data.groupby(by='bikeid')
    # print(bike_trajectory)
    # 对bike_trajectory添加上一个站点的id：prev_geo_id
    bike_trajectory = pd.concat(
        map(lambda x: add_previous_poi(x[1]), bike_trajectory))
    # 若起点和终点重合，则drop这一行
    bike_trajectory = bike_trajectory[
        bike_trajectory['geo_id'] != bike_trajectory['prev_geo_id']]
    # bike_trajectory的列包括：,bikeid,geo_id,time,prev_geo_id
    # bike_trajectory.to_csv('output/NYC_BIKE_flow_test/NYC_BIKE_bike_traj.csv')

    # 表连接操作，suffixes同名列增加何种后缀加以区分
    bike_trajectory = pd.merge(bike_trajectory, station_with_id,
                               left_on='prev_geo_id',
                               right_on='s_id', suffixes=['', '_p'])

    # 改列名
    bike_trajectory = bike_trajectory.rename(
        columns={'row_id': 'prev_row_id',
                 'column_id': 'prev_column_id', 's_id': 's_id_p'})
    # 表连接操作，连接
    bike_trajectory = pd.merge(bike_trajectory,
                               station_with_id,
                               left_on='geo_id',
                               right_on='s_id', suffixes=['', '_n'])
    # 改列名
    bike_trajectory = bike_trajectory.rename(
        columns={'s_id': 's_id_n'})
    bike_trajectory = bike_trajectory[
        (bike_trajectory.prev_row_id != bike_trajectory.row_id) |
        (bike_trajectory.prev_column_id != bike_trajectory.column_id)]
    # bike_trajectory.to_csv('output/NYC_BIKE_flow_test/NYC_BIKE_bike_traj_merge.csv')
    # 将自行车路线表根据timestamp排序
    bike_trajectory = bike_trajectory.sort_values(by='timestamp')
    # 获得时间戳的最小最大值，以interval为颗粒度。
    min_timestamp = float(
        math.floor(
            bike_trajectory['timestamp'].values[0] / interval) * interval)
    # print(min_timestamp)
    max_timestamp = float(
        math.ceil(
            bike_trajectory['timestamp'].values[-1] / interval) * interval)
    time_dividing_point = \
        list(np.arange(min_timestamp, max_timestamp, interval))
    # print(max_timestamp)
    # 为bike_trajectory加上time_id
    bike_trajectory = judge_time_id(bike_trajectory, time_dividing_point)
    # bike_trajectory.to_csv('output/NYC_BIKE_flow_test/NYC_BIKE_bike_traj_with_time.csv')
    # 接下来需要根据bike_trajectory和time_dividing_point数组统计出入流量
    flow_data_part = gen_flow_data1(bike_trajectory, time_dividing_point)
    # print("type of data part:" + str(type(flow_data_part)))
    flow_data = pd.concat(flow_data_part)
    # ,row_id,column_id,inflow,outflow,time
    # flow_data.to_csv('output/NYC_BIKE_flow_test/NYC_BIKE_flow1.csv')

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


def dc_bike_flow(
        output_dir, output_name, data_set, row_num, col_num, interval=3600):
    data_name = output_dir + "/" + output_name
    # geo data
    # generate:
    # 1. geo_data: target data.
    #   generated by row_num, col_num and the boundary of stations
    # 2. station_with_id: assistant data.
    #   station data + corresponding row_num and col_num,

    # 预处理数据集，从数据集中整理出与station有关的列，起点终点信息进行合并
    station = handle_point_geo(data_set)
    # 正式处理数据集,获得geo中的row_id和column_id，进而可以获得geo_id
    station_with_id, geo_data = partition_to_grid(station, row_num, col_num)
    station_rowcol = station_with_id[['s_id', 'row_id', 'column_id']]
    station_rowcol = station_rowcol.set_index(keys=['s_id'])
    # station_with_id.to_csv(data_name + 'station.geo', index=False)

    geo_data.to_csv(data_name + '.geo', index=False)

    print('finish geo')

    # trajectory data
    # include ['bikeid', 'geo_id', 'time', 'timestamp']

    trajectory_data = convert_to_trajectory(data_set)
    # trajectory_data.to_csv(data_name + 'trajectory_data.geo', index=False)
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
    print(config)


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
    row_num = 16
    # 列数
    column_num = 8
    # 输入文件夹名称
    input_dir_flow = 'input/BikeDC'
    # 输出文件名称 与 输出文件夹名称
    file_name = 'BIKE-DC%d%02d-%d%02d' \
                % (start_year, start_month, end_year, end_month)
    output_dir_flow = 'output/BikeDC%d%02d-%d%02d' \
                      % (start_year, start_month, end_year, end_month)
    # 创建输出文件夹
    if not os.path.exists(output_dir_flow):
        os.makedirs(output_dir_flow)

    # 待处理的数据文件名
    data_url = get_data_url(input_dir_flow=input_dir_flow,
                            start_year=start_year,
                            start_month=start_month,
                            end_year=end_year,
                            end_month=end_month
                            )
    print(data_url)
    # 读入csv文件并实现拼接
    data_set_dc = pd.concat(
        map(lambda x: add_station_loc(pd.read_csv(x)), data_url), axis=0
    )  # 纵向拼接数据
    data_set_dc.reset_index(drop=True, inplace=True)
    print('finish read csv')

    data_set_dc = data_set_dc.dropna(axis=0, subset=['start station latitude',
                                                     'start station longitude',
                                                     'end station latitude',
                                                     'end station longitude',
                                                     'start station name',
                                                     'start station id',
                                                     'end station name',
                                                     'end station id'
                                                     ])

    # 过滤不属于时间范围内的记录
    start_str = '%d-%02d-%02d' % (start_year, start_month, start_day)
    end_str = '%d-%02d-%02d' % (end_year, end_month, end_day)

    data_set_dc = data_set_dc.loc[
        data_set_dc['starttime'].apply(
            lambda x: end_str >= x.split(" ")[0] >= start_str)]
    data_set_dc = data_set_dc.loc[
        data_set_dc['stoptime'].apply(
            lambda x: end_str >= x.split(" ")[0] >= start_str)]

    # 调用处理函数，生成.grid 和.geo文件
    dc_bike_flow(
        output_dir_flow,
        file_name,
        data_set_dc,
        row_num,
        column_num,
        interval=interval
    )
    # 生成config.json文件
    gen_config(output_dir_flow, file_name, row_num, column_num, interval)
    print('finish')
    end_time = time.time()
    print(end_time - start_time)
