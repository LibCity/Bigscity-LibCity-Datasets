import h5py
import numpy as np
import util
import pandas as pd
import json


def load_h5(filename, keywords):
    f = h5py.File(filename, 'r')
    data = []
    for name in keywords:
        data.append(np.array(f[name]))
    f.close()
    if len(data) == 1:
        return data[0]
    return data


outputdir = 'output/T-Drive20150206'
util.ensure_dir(outputdir)

dataurl = 'input/T-Drive20150206/'
dataname = outputdir + '/T-Drive20150206'

x0, y0 = 116.25, 39.83
x1, y1 = 116.64, 40.12
rows, cols = 32, 32
size_x, size_y = (x1 - x0) / rows, (y1 - y0) / cols

grids = []
for r in range(rows):
    for c in range(cols):
        _x0, _y0 = c * size_x + x0, (rows - r - 1) * size_y + y0
        _x1, _y1 = (c + 1) * size_x + x0, (rows - r) * size_y + y0
        grids += [[[_x0, _y1], [_x1, _y1], [_x1, _y0], [_x0, _y0], [_x0, _y1]]]

geo_dataset = load_h5(dataurl + 'BJ_FEATURE.h5', ['embeddings'])
geo = []
x = geo_dataset.shape[0]
y = geo_dataset.shape[1]
for i in range(x):
    for j in range(y):
        id = i * y + j
        geo.append([id, 'Polygon', '[' + str(grids[id]) + ']', i, j] + [geo_dataset[i][j][k] for k in
                                                                        range(geo_dataset.shape[2])])
geo = pd.DataFrame(geo, columns=['geo_id', 'type', 'coordinates', 'row_id', 'column_id'] + [f'geo_feature_{i}' for i in
                                                                                            range(
                                                                                                geo_dataset.shape[2])])
geo.to_csv(dataname + '.geo', index=False)

rel = []
reldict = dict()
rel_dataset = load_h5(dataurl + 'BJ_GRAPH.h5', ['data'])
rel_id = 0
for i in range(rel_dataset.shape[0]):
    for j in range(rel_dataset.shape[1]):
        rel.append([rel_id, 'geo', i, j] + [rel_dataset[i][j][k] for k in range(rel_dataset.shape[2])])
        rel_id += 1
rel = pd.DataFrame(rel, columns=['rel_id', 'type', 'origin_id', 'destination_id'] + [f'rel_feature_{i}' for i in
                                                                                     range(rel_dataset.shape[2])])
rel.to_csv(dataname + '.rel', index=False)

start_timestamp = util.datetime_timestamp("2015-02-01T00:00:00Z")
grid_id = 0
grid_dataset = load_h5(dataurl + 'BJ_FLOW.h5', ['data'])
grid_file = open(dataname + '.grid', 'w')
grid_file.write(
    'dyna_id' + ',' + 'type' + ',' + 'time' + ',' + 'row_id' + ',' + 'column_id' + ',' + 'inflow' + ',' + 'outflow' +
    '\n')
for i in range(grid_dataset.shape[2]):
    for j in range(grid_dataset.shape[3]):
        for date in range(grid_dataset.shape[0]):
            for t in range(grid_dataset.shape[1]):
                time = util.timestamp_datetime(start_timestamp + (date * 24 + t) * 3600)
                grid_file.write(
                    str(grid_id) + ',' + 'state' + ',' + str(time) + ',' + str(i) + ',' + str(j) + ',' + str(
                        grid_dataset[date][t][i][j][0]) + ',' + str(grid_dataset[date][t][i][j][1]) + '\n')
                grid_id += 1
grid_file.close()

config = dict()
config['geo'] = dict()
config['geo']['including_types'] = ['Polygon']
geo_polygon_dict = {"row_id": "num", "column_id": "num"}
for i in range(989):
    geo_polygon_dict[f"geo_feature_{i}"] = "num"
config['geo']['Polygon'] = geo_polygon_dict
config['rel'] = dict()
config['rel']['including_types'] = ['geo']
rel_geo_dict = dict()
for i in range(32):
    rel_geo_dict[f"rel_feature_{i}"] = "num"
config['rel']['geo'] = rel_geo_dict
config['grid'] = dict()
config['grid']['including_types'] = ['state']
config['grid']['state'] = {'row_id': 32, 'column_id': 32, 'inflow': 'num', 'outflow': 'num'}
config['info'] = dict()
config['info']['data_col'] = ['inflow', 'outflow']
config['info']['data_files'] = ['T-Drive20150206']
config['info']['geo_file'] = 'T-Drive20150206'
config['info']['rel_file'] = 'T-Drive20150206'
config['info']['output_dim'] = 2
config['info']['init_weight_inf_or_zero'] = 'inf'
config['info']['set_weight_link_or_dist'] = 'dist'
config['info']['calculate_weight_adj'] = False
config['info']['weight_adj_epsilon'] = 0.1
json.dump(config, open(outputdir + '/config.json', 'w', encoding='utf-8'), ensure_ascii=False)
