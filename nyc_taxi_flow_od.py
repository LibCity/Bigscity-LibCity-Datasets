import json
import math
import os
from datetime import datetime

import numpy as np
import pandas as pd

old_time_format = '%Y-%m-%d %H:%M:%S'
new_time_format = '%Y-%m-%dT%H:%M:%SZ'


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


def handle_area_geo(df):
    """
    :param df:
    :return: df['a_id']
    """
    start = df[['PULocationID']]
    start.columns = ['a_id']
    end = df[['DOLocationID']]
    end.columns = ['a_id']
    area_data = pd.concat((start, end), axis=0)
    area_data['a_id'] = area_data['a_id'].astype('int')
    area_data = area_data.sort_values(by='a_id', ascending=True)
    area_data = area_data[['a_id']]
    area_data = area_data.drop_duplicates()
    return area_data


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


def get_geo_data(area_geo):
    """
    :param area_geo:
    :return: df['geo_id', 'type', 'coordinates']
    """
    # generate gird data (.geo)
    geo_data = pd.DataFrame(
        columns=['geo_id', 'type', 'coordinates'])
    geo_data['geo_id'] = area_geo['a_id']
    geo_data.loc[:, 'type'] = 'state'
    geo_data.loc[:, 'coordinates'] = '[[ [] ]]'
    return geo_data


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
            pd.to_datetime(x['time_str'],
                           utc=True,
                           format=old_time_format))),
        axis=1)
    return df


def convert_to_trajectory(df):
    """
    :param df: all data
    :return: df['driveid', 'geo_id', 'time', 'timestamp']
    """
    start = df[['drive_id', 'PULocationID', 'tpep_pickup_datetime']]
    end = df[['drive_id', 'DOLocationID', 'tpep_dropoff_datetime']]
    start.columns = ['driveid', 'geo_id', 'time_str']
    end.columns = ['driveid', 'geo_id', 'time_str']
    trajectory_data = pd.concat((start, end), axis=0)
    trajectory_data = trajectory_data.loc[trajectory_data['geo_id'].apply(lambda x: not math.isnan(x))]
    trajectory_data = convert_time(trajectory_data)
    return trajectory_data[['driveid', 'geo_id', 'time', 'timestamp']]


def add_previous_poi(tra_by_taxi):
    tra_by_taxi = tra_by_taxi.sort_values(by='time')
    tra_by_taxi['prev_geo_id'] = tra_by_taxi['geo_id'].shift(1)
    tra_by_taxi['prev_time'] = tra_by_taxi['time'].shift(1)
    tra_by_taxi['prev_timestamp'] = tra_by_taxi['timestamp'].shift(1)
    return tra_by_taxi[1:]


def judge_time_id(df, time_dividing_point):
    df['time_id'] = df.apply(
        lambda x: judge_id(x['timestamp'], time_dividing_point),
        axis=1
    )
    df['prev_time_id'] = df.apply(
        lambda x: judge_id(x['prev_timestamp'], time_dividing_point),
        axis=1
    )
    return df


def timestamp2str(timestamp):
    return pd.to_datetime(timestamp, unit='s').strftime(new_time_format)


def gen_empty_od_data(area, time_dividing_point):
    od_id = 0
    od_data = pd.DataFrame(columns=['od_id', 'type', 'time',
                                    'origin_id', 'destination_id', 'flow'])
    a_ids = area['a_id']
    a_ids = range(1, 6)
    for ori_id in a_ids:
        for des_id in a_ids:
            for t in time_dividing_point:
                od_data.loc[od_id] = [od_id, 'state', timestamp2str(t), ori_id, des_id, 0]
                od_id += 1
    return od_data


def calculate_od(trajectory_data, area, interval):
    taxi_trajectory = trajectory_data.groupby(by='driveid')
    taxi_trajectory = pd.concat(
        map(lambda x: add_previous_poi(x[1]), taxi_trajectory))
    taxi_trajectory = taxi_trajectory[
        taxi_trajectory['geo_id'] != taxi_trajectory['prev_geo_id']]

    taxi_trajectory = taxi_trajectory.sort_values(by='timestamp')
    min_timestamp = float(
        math.floor(
            taxi_trajectory['timestamp'].values[0] / interval) * interval)
    max_timestamp = float(
        math.ceil(
            taxi_trajectory['timestamp'].values[-1] / interval) * interval)
    time_dividing_point = \
        list(np.arange(min_timestamp, max_timestamp, interval))
    taxi_trajectory = judge_time_id(taxi_trajectory, time_dividing_point)
    taxi_trajectory = \
        taxi_trajectory.loc[taxi_trajectory['time_id'] == taxi_trajectory['prev_time_id']]

    od_data = gen_empty_od_data(area, time_dividing_point)
    for index, row in taxi_trajectory.iterrows():
        time_id = judge_id(row['timestamp'], time_dividing_point)
        time_str = timestamp2str(time_dividing_point[time_id])
        prev_geo_id = row['prev_geo_id']
        geo_id = row['geo_id']
        od_data['flow'] = list(map(lambda time, origin_id, destination_id, flow:
                                   flow + 1 if time == time_str and origin_id == prev_geo_id and destination_id == geo_id
                                   else flow, od_data.time, od_data.origin_id, od_data.destination_id, od_data.flow))

    return od_data


def nyc_taxi_flow(
        output_dir, output_name, data_set, interval=3600):
    data_name = output_dir + "/" + output_name

    # geo data
    area = handle_area_geo(data_set)
    print('finish geo')

    # trajectory data
    trajectory_data = convert_to_trajectory(data_set)
    print('finish trajectory')

    # od data
    od_data = calculate_od(trajectory_data, area, interval=interval)
    od_data.to_csv(data_name + '.od', index=False)
    print('finish od')


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
    data['od'] = gen_config_od()
    data["info"] = gen_config_info(file_name, interval)
    config = json.dumps(data)
    with open(output_dir_flow + "/config.json", "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)
    print(config)


if __name__ == '__main__':
    interval = 3600
    (start_year, start_month) = (2020, 6)
    (end_year, end_month) = (2020, 6)

    file_name = 'NYCTAXI%d%02d-%d%02d' % (start_year, start_month, end_year, end_month)
    output_dir_flow = 'output/NYCTAXI%d%02d-%d%02d' % (start_year, start_month, end_year, end_month)
    input_dir_flow = 'input/NYC-Taxi'
    data_url = get_data_url(input_dir_flow=input_dir_flow,
                            start_year=start_year,
                            start_month=start_month,
                            end_year=end_year,
                            end_month=end_month
                            )
    data_url = tuple(data_url)
    if not os.path.exists(output_dir_flow):
        os.makedirs(output_dir_flow)

    dataset_nyc = pd.concat(
        map(lambda x: pd.read_csv(x, index_col=False), data_url), axis=0
    )
    dataset_nyc.reset_index(drop=True, inplace=True)
    data_num = dataset_nyc.shape[0]
    dataset_nyc["drive_id"] = list(range(data_num))
    dataset_nyc = dataset_nyc.loc[dataset_nyc['tpep_pickup_datetime'].
        apply(lambda x:
              '%d-%02d-%02d' % (end_year, end_month, 30) >= x[:10] >= '%d-%02d-%02d' % (start_year, start_month, 1))]
    dataset_nyc = dataset_nyc.loc[dataset_nyc['tpep_dropoff_datetime'].
        apply(lambda x:
              '%d-%02d-%02d' % (end_year, end_month, 30) >= x[:10] >= '%d-%02d-%02d' % (start_year, start_month, 1))]
    print('finish read csv')

    nyc_taxi_flow(
        output_dir_flow,
        file_name,
        dataset_nyc,
        interval=interval
    )
    print('finish')

    gen_config(output_dir_flow, file_name, interval)
