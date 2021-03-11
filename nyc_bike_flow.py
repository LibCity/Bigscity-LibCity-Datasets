import numpy as np
import pandas as pd
import json
import os
import util
import multiprocessing
import functools
import math


def handle_point_geo(df):
    """
    :param df:
    :return: df['geo_id', 'poi_name', 'poi_lat', 'poi_lon']
    """
    start = df[['start station id', 'start station name', 'start station latitude', 'start station longitude']]
    start.columns = ['s_id', 's_name', 's_lat', 's_lon']
    end = df[['end station id', 'end station name', 'end station latitude', 'end station longitude']]
    end.columns = ['s_id', 's_name', 's_lat', 's_lon']
    geo_data = pd.concat((start, end), axis=0)

    geo_data = geo_data.drop_duplicates()

    geo_data.rename(columns={'s_id': 'geo_id', 's_name': 'poi_name', 's_lat': 'poi_lat', 's_lon': 'poi_lon'}, inplace=True)
    geo_data = geo_data[['geo_id', 'poi_name', 'poi_lat', 'poi_lon']]
    geo_data = geo_data.sort_values(by='geo_id')
    return geo_data


# TODO: optimize performance
def judge_id(value, dividing_points):
    for i, num in enumerate(dividing_points):
        if value < num:
            return i - 1
    return len(dividing_points)


def partition_to_grid(point_geo, row_num, col_num):
    """
    :param point_geo:
    :param row_num:
    :param col_num:
    :return: df['geo_id', 'poi_name', 'poi_lat', 'poi_lon', 'row_id', 'column_id']
    """
    # handle row/latitude
    point_geo = point_geo.sort_values(by='poi_lat')
    lat_values = point_geo['poi_lat'].values
    lat_diff = lat_values[-1] - lat_values[0]
    lat_dividing_points = [lat_values[0] + lat_diff / row_num * i for i in range(row_num)]
    point_geo['row_id'] = point_geo.apply(
        lambda x: judge_id(x['poi_lat'], lat_dividing_points),
        axis=1
    )

    # handle col/longitude
    point_geo = point_geo.sort_values(by='poi_lon')
    lon_values = point_geo['poi_lon'].values
    lon_diff = lon_values[-1] - lon_values[0]
    lon_dividing_points = [lon_values[0] + lon_diff / col_num * i for i in range(col_num)]
    point_geo['column_id'] = point_geo.apply(
        lambda x: judge_id(x['poi_lon'], lon_dividing_points),
        axis=1
    )

    # TODO: grid data?

    return point_geo


def convert_time(df):
    df['time'], df['timestamp'] = zip(*df.apply(
        lambda x: util.add_TZ(x["time_str"], with_timestamp=True),
        axis=1
    ))
    return df


def convert_to_trajectory(df, multi=False):
    """
    :param df: all data
    :param multi:
    :return: df['bikeid', 'geo_id', 'time', 'timestamp']
    """
    start = df[['bikeid', 'start station id', 'starttime']]
    start.columns = ['bikeid', 'geo_id', 'time_str']
    end = df[['bikeid', 'end station id', 'stoptime']]
    end.columns = ['bikeid', 'geo_id', 'time_str']
    trajectory_data = pd.concat((start, end), axis=0)

    if multi:
        pool_size = multiprocessing.cpu_count()
        trajectory_split = np.array_split(trajectory_data, pool_size)
        with multiprocessing.Pool(processes=pool_size) as pool:
            trajectory_parts = pool.map(convert_time, trajectory_split)
        trajectory_data = pd.concat(trajectory_parts)
    else:
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


def gen_flow_data(trajectory, time_dividing_point, row_num, col_num):
    """
    :param trajectory:
    :param time_dividing_point:
    :param row_num:
    :param col_num:
    :return: ['time', 'row_id', 'column_id', 'inflow', 'outflow']
    """
    tra_groups = trajectory.groupby(by='time_id')
    for tra_group, t in zip(tra_groups, time_dividing_point):
        tra_group = tra_group[1]
        flow_in = tra_group.groupby(by=['row_id', 'column_id'])[['geo_id']].count().sort_index()
        flow_in.columns = ['inflow']
        flow_out = tra_group.groupby(by=['prev_row_id', 'prev_column_id'])[['prev_geo_id']].count().sort_index()
        flow_out.index.names = ['row_id', 'column_id']
        flow_out.columns = ['outflow']
        flow = flow_in.join(flow_out, how='outer', on=['row_id', 'column_id'])
        flow = flow.fillna(value={'inflow': 0, 'outflow': 0})
        flow = flow.reset_index()
        flow['time'] = util.timestamp_to_str(t)
        yield flow
        # flow_in = np.zeros((row_num, col_num), dtype=np.int)
        # flow_out = np.zeros((row_num, col_num), dtype=np.int)
        # for tra in tra_group:
        #     flow_in[tra['row_id']][tra['column_id']] += 1
        #     flow_out[tra['prev_row_id']][tra['prev_column_id']] += 1
        # for rid, row_flow in enumerate(zip(flow_in, flow_out)):
        #     for cid, flow in enumerate(zip(*row_flow)):
        #         yield t, rid, cid, flow[0], flow[1]


def calculate_flow(trajectory_data, point_geo, row_num, col_num, interval=3600, multi=False):
    point_geo = point_geo[['geo_id', 'row_id', 'column_id']]
    bike_trajectory = trajectory_data.groupby(by='bikeid')
    bike_trajectory = pd.concat(map(lambda x: add_previous_poi(x[1]), bike_trajectory))
    bike_trajectory = bike_trajectory[bike_trajectory['geo_id'] != bike_trajectory['prev_geo_id']]

    bike_trajectory = pd.merge(bike_trajectory, point_geo,
                               left_on='prev_geo_id', right_on='geo_id', suffixes=['', '_p'])
    bike_trajectory = bike_trajectory.rename(columns={'row_id': 'prev_row_id', 'column_id': 'prev_column_id'})
    bike_trajectory = pd.merge(bike_trajectory, point_geo, left_on='geo_id', right_on='geo_id', suffixes=['', '_n'])

    bike_trajectory = bike_trajectory.sort_values(by='timestamp')
    min_timestamp = int(math.floor(bike_trajectory['timestamp'].values[0] / interval) * interval)
    max_timestamp = int(math.ceil(bike_trajectory['timestamp'].values[-1] / interval) * interval)
    time_dividing_point = list(range(min_timestamp, max_timestamp, 3600))
    if multi:
        pool_size = multiprocessing.cpu_count()
        trajectory_split = np.array_split(bike_trajectory, pool_size)
        judge_time_func = functools.partial(judge_time_id, time_dividing_point=time_dividing_point)
        with multiprocessing.Pool(processes=pool_size) as pool:
            trajectory_parts = pool.map(judge_time_func, trajectory_split)
        bike_trajectory = pd.concat(trajectory_parts)
    else:
        bike_trajectory = judge_time_id(bike_trajectory, time_dividing_point)

    flow_data_part = gen_flow_data(bike_trajectory, time_dividing_point, row_num, col_num)
    flow_data = pd.concat(flow_data_part)
    flow_data['type'] = 'state'
    flow_data = flow_data.reset_index(drop=True)
    flow_data['dyna_id'] = flow_data.index

    flow_data = flow_data[['dyna_id', 'type', 'time', 'row_id', 'column_id', 'inflow', 'outflow']]
    return flow_data


def nyc_bike_flow(output_dir, dataset, row_num, col_num, interval=3600, multi=False):
    data_name = output_dir + '/NYC_BIKE'

    # geo data
    geo = handle_point_geo(dataset)
    geo_with_id = partition_to_grid(geo, row_num, col_num)
    # geo.to_csv(data_name + '.geo', index=False)
    print('finish geo')

    # trajectory data
    trajectory_data = convert_to_trajectory(dataset, multi=multi)
    print('finish trajectory')

    # flow data
    flow_data = calculate_flow(trajectory_data, geo_with_id, row_num, col_num, interval=interval, multi=multi)
    flow_data.to_csv(data_name + '.grid', index=False)
    print('finish flow')

    config = dict()
    config['geo'] = dict()
    config['geo']['including_types'] = ['Point']
    config['geo']['Point'] = {'poi_name': 'other'}
    config['usr'] = dict()
    config['usr']['properties'] = {'usertype': 'other', 'birth_year': 'num', 'gender': 'num'}
    config['rel'] = dict()
    config['rel']['including_types'] = ['geo']
    config['rel']['geo'] = {}
    config['dyna'] = dict()
    config['dyna']['including_types'] = ['od']
    config['dyna']['od'] = {'entity_id': 'usr_id', 'od_id': 'rel_id', 'bikeid': 'other', 'trip_duration': 'num'}
    json.dump(config, open(output_dir + '/config.json', 'w', encoding='utf-8'), ensure_ascii=False)


if __name__ == '__main__':
    output_dir_flow = 'output/NYC_BIKE_flow_test'
    util.ensure_dir(output_dir_flow)

    data_url = (
        'input/NYC-Bike/JC-202009-citibike-tripdata.csv',
        # 'input/NYC-Bike/2014-04 - Citi Bike trip data.csv',
        # 'input/NYC-Bike/2014-05 - Citi Bike trip data.csv',
        # 'input/NYC-Bike/2014-06 - Citi Bike trip data.csv',
        # 'input/NYC-Bike/2014-07 - Citi Bike trip data.csv',
        # 'input/NYC-Bike/2014-08 - Citi Bike trip data.csv',
        # 'input/NYC-Bike/201409-citibike-tripdata.csv',
    )
    dataset_nyc = pd.concat(map(lambda x: pd.read_csv(x), data_url), axis=0)
    dataset_nyc.reset_index(drop=True, inplace=True)
    print('finish read csv')

    nyc_bike_flow(output_dir_flow, dataset_nyc, 16, 8, interval=3600, multi=False)
    print('finish')
