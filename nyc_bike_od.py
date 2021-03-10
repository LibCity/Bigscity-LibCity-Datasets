import numpy as np
import pandas as pd
import json
import os
import util
import multiprocessing
import functools


def handle_geo(df):
    start = df[['start station id', 'start station name', 'start station latitude', 'start station longitude']]
    start.columns = ['s_id', 's_name', 's_lat', 's_lon']
    end = df[['end station id', 'end station name', 'end station latitude', 'end station longitude']]
    end.columns = ['s_id', 's_name', 's_lat', 's_lon']
    geo_data = pd.concat((start, end), axis=0)

    geo_data = geo_data.drop_duplicates()
    geo_data['s_coor'] = geo_data.apply(lambda x: f'[{str(x["s_lon"])}, {str(x["s_lat"])}]', axis=1)
    geo_data.drop(labels=['s_lat', 's_lon'], axis=1, inplace=True)
    geo_data['type'] = 'Point'

    geo_data.rename(columns={'s_id': 'geo_id', 's_coor': 'coordinates', 's_name': 'poi_name'}, inplace=True)
    geo_data = geo_data[['geo_id', 'type', 'coordinates', 'poi_name']]
    geo_data = geo_data.sort_values(by='geo_id')
    return geo_data


def handle_usr(df):
    usr_data = df[['usertype', 'birth year', 'gender']]
    usr_data = usr_data.reset_index()

    usr_data.rename(columns={'birth year': 'birth_year', 'index': 'usr_id'}, inplace=True)
    usr_data = usr_data[['usr_id', 'usertype', 'birth_year', 'gender']]
    return usr_data


def handle_rel(df):
    rel_data = df[['start station id', 'end station id']]
    rel_data = rel_data.drop_duplicates()
    rel_data = rel_data.reset_index(drop=True)
    rel_data['index'] = rel_data.index
    rel_data['type'] = 'geo'

    rel_data.rename(
        columns={'index': 'rel_id', 'start station id': 'origin_id', 'end station id': 'destination_id'},
        inplace=True
    )
    rel_data = rel_data[['rel_id', 'type', 'origin_id', 'destination_id']]
    return rel_data


def handle_dyna(df, rel_df, multi=False):
    dyna_data = df[['tripduration', 'starttime', 'stoptime', 'start station id', 'end station id', 'bikeid']]
    dyna_data = dyna_data.reset_index(drop=True)
    dyna_data['index'] = dyna_data.index
    dyna_data['entity_id'] = dyna_data['index']
    dyna_data['type'] = 'od'
    dyna_data.rename(
        columns={
            'index': 'dyna_id',
            'tripduration': 'trip_duration',
            'start station id': 'origin_id',
            'end station id': 'destination_id',
        },
        inplace=True,
    )
    rel_ids = rel_df[['rel_id', 'origin_id', 'destination_id']]

    if multi:
        pool_size = multiprocessing.cpu_count()
        dyna_split = np.array_split(dyna_data, pool_size)
        multi_handler = functools.partial(add_time_and_join, rel_ids=rel_ids)
        with multiprocessing.Pool(processes=pool_size) as pool:
            dyna_parts = pool.map(multi_handler, dyna_split)
        dyna_data = pd.concat(dyna_parts)
    else:
        dyna_data = add_time_and_join(dyna_data, rel_ids=rel_ids)

    dyna_data.rename(columns={'rel_id': 'od_id'}, inplace=True)
    dyna_data = dyna_data[['dyna_id', 'type', 'time', 'entity_id', 'od_id', 'bikeid', 'trip_duration']]
    dyna_data = dyna_data.sort_values(by='dyna_id')
    return dyna_data


def add_time_and_join(df, rel_ids=None):
    if rel_ids is None:
        raise ValueError('Empty rel value')
    df['time'] = df.apply(
        lambda x: f'{util.add_TZ(x["starttime"])} {util.add_TZ(x["stoptime"])}',
        axis=1
    )
    merged_df = pd.merge(df, rel_ids)
    return merged_df


if __name__ == '__main__':
    output_dir = 'output/NYC_BIKE_od_new'
    util.ensure_dir(output_dir)

    data_url = (
        # 'input/NYC-Bike/JC-202009-citibike-tripdata.csv',
        'input/NYC-Bike/2014-04 - Citi Bike trip data.csv',
        'input/NYC-Bike/2014-05 - Citi Bike trip data.csv',
        'input/NYC-Bike/2014-06 - Citi Bike trip data.csv',
        'input/NYC-Bike/2014-07 - Citi Bike trip data.csv',
        'input/NYC-Bike/2014-08 - Citi Bike trip data.csv',
        'input/NYC-Bike/201409-citibike-tripdata.csv',
    )
    data_name = output_dir + '/NYC_BIKE'

    dataset = pd.concat(map(lambda x: pd.read_csv(x), data_url), axis=0)
    dataset.reset_index(drop=True, inplace=True)
    print('finish read csv')

    # geo data
    geo = handle_geo(dataset)
    geo.to_csv(data_name + '.geo', index=False)
    print('finish geo')

    # usr data
    usr = handle_usr(dataset)
    usr.to_csv(data_name + '.usr', index=False)
    print('finish usr')

    # rel data
    rel = handle_rel(dataset)
    rel.to_csv(data_name + '.rel', index=False)
    print('finish rel')

    # dyna data
    dyna = handle_dyna(dataset, rel, multi=True)
    dyna.to_csv(data_name + '.dyna', index=False)
    print('finish dyna')

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
