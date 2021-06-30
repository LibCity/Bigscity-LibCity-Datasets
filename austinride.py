# link: https://data.world/ride-austin/ride-austin-june-6-april-13
import json
import math
import os
from datetime import datetime
import time
import pandas as pd

old_time_format = '%Y-%m-%d %H:%M:%S'
new_time_format = '%Y-%m-%dT%H:%M:%SZ'


def handle_point_geo(df):
    """
    :param df:
    :return: df['geo_id', 'poi_lat', 'poi_lon']
    """
    start = df[['start_location_lat', 'start_location_long']]
    start.columns = ['s_lat', 's_lon']
    end = df[['end_location_lat', 'end_location_long']]
    end.columns = ['s_lat', 's_lon']
    station_data = pd.concat((start, end), axis=0)
    station_data = station_data.drop_duplicates()
    station_data.rename(columns={'s_lat': 'poi_lat', 's_lon': 'poi_lon'},
                        inplace=True)

    station_data = station_data.loc[station_data['poi_lat'].apply(lambda x: x != 0 and x is not None and not math.isnan(x))]
    station_data = station_data.loc[station_data['poi_lon'].apply(lambda x: x != 0 and x is not None and not math.isnan(x))]
    station_num = station_data.shape[0]
    station_data.loc[:, 'geo_id'] = range(0, station_num)
    station_data = station_data[['geo_id', 'poi_lat', 'poi_lon']]

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
    :return: df['geo_id', 'poi_lat', 'poi_lon', 'row_id', 'column_id']
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
    """
    old_time_format = '%Y-%m-%d %H:%M:%S'
    new_time_format = '%Y-%m-%dT%H:%M:%SZ'
    """

    df['time'] = df.apply(
        lambda x: x['time_str'].replace(' ', 'T') + 'Z',
        axis=1)
    df['timestamp'] = df.apply(
        lambda x: float(datetime.timestamp(
            pd.to_datetime(x['time_str'][:-6],
                           utc=True,
                           format=old_time_format))),
        axis=1)
    return df


def convert_to_trajectory(df):
    """
    :param df: all data
    :return: df['riderid', 'poi_lon', 'poi_lat', 'time', 'timestamp']
    """
    start = df[['rider_id', 'start_location_long', 'start_location_lat', 'started_on']]
    start.columns = ['riderid', 'poi_lon', 'poi_lat', 'time_str']
    end = df[['rider_id', 'end_location_long', 'end_location_lat', 'completed_on']]
    end.columns = ['riderid', 'poi_lon', 'poi_lat', 'time_str']
    trajectory_data = pd.concat((start, end), axis=0)
    trajectory_data = convert_time(trajectory_data)
    trajectory_data = trajectory_data.loc[trajectory_data['poi_lat'].apply(lambda x: x != 0)]
    trajectory_data = trajectory_data.loc[trajectory_data['poi_lon'].apply(lambda x: x != 0)]
    return trajectory_data[['riderid', 'poi_lon', 'poi_lat', 'time', 'timestamp']]


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
        (trajectory['row_id'] != trajectory['row_id_p']) |
        (trajectory['column_id'] != trajectory['column_id_p'])
        ]
    tra_groups = trajectory.groupby(by='time_id')

    for tra_group, t in zip(tra_groups, time_dividing_point):
        tra_group = tra_group[1]
        flow_in = tra_group.groupby(by=['row_id', 'column_id'])[['geo_id']].count().sort_index()
        flow_in.columns = ['inflow']
        flow_out = tra_group.groupby(by=['row_id_p', 'column_id_p'])[['geo_id_p']].count().sort_index()
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
        trajectory_data, point_geo, row_num, col_num, interval):
    point_geo = point_geo[['geo_id', 'row_id', 'column_id']]
    bike_trajectory = trajectory_data.groupby(by='riderid')
    bike_trajectory = pd.concat(map(lambda x: add_previous_poi(x[1]), bike_trajectory))
    bike_trajectory = bike_trajectory[bike_trajectory['geo_id'] != bike_trajectory['prev_geo_id']]

    bike_trajectory = pd.merge(bike_trajectory, point_geo,
                               left_on='prev_geo_id', right_on='geo_id', suffixes=['', '_p'])

    bike_trajectory = bike_trajectory.sort_values(by='timestamp')
    min_timestamp = int(math.floor(bike_trajectory['timestamp'].values[0] / interval) * interval)
    max_timestamp = int(math.ceil(bike_trajectory['timestamp'].values[-1] / interval) * interval)
    time_dividing_point = list(range(min_timestamp, max_timestamp, interval))
    bike_trajectory = judge_time_id(bike_trajectory, time_dividing_point)

    flow_data_part = gen_flow_data(bike_trajectory, time_dividing_point)
    flow_data = pd.concat(flow_data_part)
    flow_data = fill_empty_flow(flow_data, time_dividing_point, row_num, col_num)
    flow_data = flow_data.fillna(value={'inflow': 0, 'outflow': 0})
    flow_data['type'] = 'state'
    flow_data = flow_data.reset_index(drop=True)
    flow_data['dyna_id'] = flow_data.index

    flow_data = flow_data[['dyna_id', 'type', 'time', 'row_id', 'column_id', 'inflow', 'outflow']]
    return flow_data


def austin_bike_flow(
        output_dir, output_name, data_set, row_num, col_num, interval=3600):
    data_name = output_dir + "/" + output_name

    # geo data
    station = handle_point_geo(data_set)
    station_with_id, geo_data = partition_to_grid(station, row_num, col_num)
    geo_data.to_csv(data_name + '.geo', index=False)
    print('finish geo')

    # trajectory data
    trajectory_data = convert_to_trajectory(data_set)
    trajectory_data = pd.merge(trajectory_data, station_with_id, how='outer')
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


if __name__ == '__main__':
    start_time = time.time()
    interval = 3600
    (start_year, start_month, start_day) = (2016, 7, 1)
    (end_year, end_month, end_day) = (2016, 9, 30)
    row_num = 16
    column_num = 8

    file_name = 'AUSTINRIDE%d%02d%02d-%d%02d%02d' % \
                (start_year, start_month, start_day, end_year, end_month, end_day)
    output_dir_flow = 'output/AUSTINRIDE%d%02d%02d-%d%02d%02d' % \
                      (start_year, start_month, start_day, end_year, end_month, end_day)
    input_dir_flow = 'input/AUSTINRIDE'
    data_url = [input_dir_flow + "/Rides_DataA.csv"]
    data_url = tuple(data_url)
    if not os.path.exists(output_dir_flow):
        os.makedirs(output_dir_flow)

    dataset_austin = pd.concat(
        map(lambda x: pd.read_csv(x), data_url),
        axis=0
    )
    start_str = '%d-%02d-%02d' % (start_year, start_month, start_day)
    end_str = '%d-%02d-%02d' % (end_year, end_month, end_day)
    dataset_austin = \
        dataset_austin.loc[dataset_austin['started_on'].apply(lambda x: end_str >= x.split(" ")[0] >= start_str)]
    dataset_austin = \
        dataset_austin.loc[dataset_austin['completed_on'].apply(lambda x: end_str >= x.split(" ")[0] >= start_str)]
    dataset_austin.reset_index(drop=True, inplace=True)
    print('finish read csv')

    austin_bike_flow(
        output_dir_flow,
        file_name,
        dataset_austin,
        row_num,
        column_num,
        interval=interval
    )
    print('finish')

    gen_config(output_dir_flow, file_name, row_num, column_num, interval)
    end_time = time.time()
    print(end_time - start_time)
