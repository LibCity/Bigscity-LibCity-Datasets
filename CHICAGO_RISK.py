# link: https://github.com/Echohhhhhh/GSNet
import os

import numpy as np
import pandas as pd
import datetime
import json
import pickle
import sys


dataurl, outputdir, prefix = 'input/CHICAGO_RISK', 'output/CHICAGO_RISK', 'output/CHICAGO_RISK/CHICAGO_RISK'


row_count, column_count = 20, 20
graph_node_count = 197


geo_columns = ['geo_id', 'type', 'coordinates', 'row_id', 'column_id', 'risk_mask']
rel_columns = ['rel_id', 'type', 'origin_id', 'destination_id', 'road_adj', 'risk_adj']
dyna_columns = ['dyna_id', 'type', 'time', 'row_id', 'column_id', 'risk', 'holiday',
            'temperature', *[f'weather_{n}' for n in ['clear', 'cloudy', 'rain', 'snow', 'mist']],
            'inflow', 'outflow']

geo_type = 'Polygon'
rel_type = 'geo'
dyna_type = 'state'


def write_geo() -> None:
    # write graph nodes instead of grids
    geo_file = open(prefix + '.geo', 'w')
    with open(dataurl + '/grid_node_map.pkl', 'rb') as f:
        grid_node_map = pickle.load(f)

    with open(dataurl + '/risk_mask.pkl', 'rb') as f:
        rm = pickle.load(f)

    effective_node = []
    for i in range(grid_node_map.shape[0]):
        grid = grid_node_map[i]
        if np.sum(grid) != 0:
           effective_node.append(i)



    geo_file.write(','.join(geo_columns))
    geo_file.write('\n')
    i = 0
    for nid in range(graph_node_count):
        row_id = effective_node[i] // 20
        column_id = effective_node[i] % 20
        row = [nid, geo_type, [], row_id, column_id, rm[column_id, row_id]]
        geo_file.write(','.join(map(str, row)))
        geo_file.write('\n')
        i += 1

    geo_file.close()


def write_rel() -> None:
    rel_file = open(prefix + '.rel', 'w')

    rel_file.write(','.join(rel_columns))
    rel_file.write('\n')

    adjs = {}
    for k in ['road_adj', 'risk_adj']:
        with open(dataurl + '/' + k + '.pkl', 'rb') as f:
            adjs[k] = pickle.load(f)

    rel_id = 0
    # orig_id points to the first dimension of the grid
    for i in range(graph_node_count):
        for j in range(graph_node_count):
            row = [
                rel_id,
                rel_type,
                i,
                j,
                adjs['road_adj'][i][j],
                adjs['risk_adj'][i][j]
            ]
            rel_file.write(','.join(map(str, row)))
            rel_file.write('\n')

            rel_id += 1

    del adjs
    rel_file.close()


def write_dyna() -> None:
    dyna_file = open(prefix + '.grid', 'w')

    with open(dataurl + '/all_data.pkl', 'rb') as f:
        ad = pickle.load(f)

    dyna_file.write(','.join(dyna_columns))
    dyna_file.write('\n')

    dyna_id = 0
    time_slots, num_features, num_cols, num_rows = ad.shape

    for r in range(num_rows):
        for c in range(num_cols):
            for ts in range(time_slots):
                dt = datetime.datetime(2016, 2, 1, 0, 0) + datetime.timedelta(days=ts//24, hours=ts%24)
                dt: str = dt.isoformat() + 'Z'
                curr_row_raw = ad[ts, :, c, r]

                row = [
                        dyna_id,
                        dyna_type,
                        dt,
                        r,
                        c,
                        curr_row_raw[0],
                        curr_row_raw[32],
                        curr_row_raw[33],
                        *[curr_row_raw[34+i] for i in range(5)],
                        curr_row_raw[39],
                        curr_row_raw[40]
                ]

                for i in range(len(row)):
                    if isinstance(row[i], str):
                        continue
                    elif isinstance(row[i], int):
                        row[i] = repr(row[i])
                    elif isinstance(row[i], float):
                        row[i] = repr(row[i])
                    else:
                        raise TypeError()
                dyna_file.write(','.join(row))
                dyna_file.write('\n')

                dyna_id += 1

                if dyna_id % 16384 == 0:
                    print(f'{dyna_id}/{time_slots * num_cols * num_rows}', file=sys.stderr)

    del ad
    dyna_file.close()


def write_config() -> None:
    config = {
        'geo': {
            'including_types': geo_type,
            geo_type: {
                'row_id': 'num',
                'column_id': 'num',
                'risk_mask': 'num'
            }
        },
        'rel': {
            'including_types': rel_type,
            rel_type: {
                'road_adj': 'num',
                'risk_adj': 'num'
            }
        },
        'grid': {
            'including_types': dyna_type,
            dyna_type: {
                'row_id': 20,
                'column_id': 20,
                'risk': 'num',
                'holiday': 'num',
                'temperature': 'num',
                'weather_clear': 'num',
                'weather_cloudy': 'num',
                'weather_rain': 'num',
                'weather_snow': 'num',
                'weather_mist': 'num',
                'inflow': 'num',
                'outflow': 'num'
            }
        },
        'info': {
            'data_col': dyna_columns[5:],
            'weight_col': rel_columns[4:],
            'data_files': ['CHICAGO_RISK'],
            'graph_input_col': [
                'risk',
                'inflow',
                'outflow'
            ],
            'target_time_col': [
                'holiday'
            ],
            'geo_file': 'CHICAGO_RISK',
            'rel_file': 'CHICAGO_RISK',
            'grid_len_row': row_count,
            'grid_len_column': column_count,
            'output_dim': 1,
            'time_intervals': 3600,
            'risk_thresholds': [0, 0.04, 0.08],
            'risk_weights': [0.05, 0.2, 0.25, 0.5]
        }
    }

    with open(outputdir + '/config.json', 'w', encoding='UTF-8') as f:
        json.dump(config, f, indent=4, ensure_ascii=False)


if __name__ == '__main__':
    if not os.path.exists(outputdir):
        os.mkdir(outputdir)
    write_geo()
    write_rel()
    write_dyna()
    write_config()
