import util
import numpy as np
import pandas as pd
import json

# outputdir = 'output/NYCBike20160708'
outputdir = '../NYCBike20160708'
util.ensure_dir(outputdir)

# dataurl = 'input/NYCBike20160708/'
dataurl = '../NYCBike20160708/'
dataname = outputdir + '/NYCBike20160708'

dataset = np.load(open(dataurl + "bike_volume_train.npz", "rb"))["volume"]
idset = set()
geo = []
x = dataset.shape[1]
y = dataset.shape[2]
for i in range(x):
    for j in range(y):
        id = i * y + j
        idset.add(id)
        geo.append([str(id), 'Polygon', '[]', str(i), str(j)])
geo = pd.DataFrame(geo, columns=['geo_id', 'type', 'coordinates', 'row_id', 'column_id'])
geo.to_csv(dataname + '.geo', index=False)

grid_id = 0
grid_file = open(dataname + '.grid', 'w')
grid_file.write(
    'dyna_id' + ',' + 'type' + ',' + 'time' + ',' + 'row_id' + ',' + 'column_id' + ',' + 'departing_volume' + ',' +
    'arriving_volume' + '\n')
volume_train_dataset = np.load(open(dataurl + "bike_volume_train.npz", "rb"))["volume"]
volume_test_dataset = np.load(open(dataurl + "bike_volume_test.npz", "rb"))["volume"]
train_time = volume_train_dataset.shape[0]
test_time = volume_test_dataset.shape[0]
train_start_timestamp = util.datetime_timestamp("2016-07-01T00:00:00Z")
test_start_timestamp = util.datetime_timestamp("2016-08-10T00:00:00Z")
x = volume_train_dataset.shape[1]
y = volume_train_dataset.shape[2]
for i in range(x):
    for j in range(y):
        for t in range(train_time):
            grid_file.write(str(grid_id) + ',' + 'state' + ',' + str(
                util.timestamp_datetime(train_start_timestamp + t * 1800)) + ',' + str(i) + ',' + str(j) + ',' + str(
                volume_train_dataset[t][i][j][0]) + ',' + str(volume_train_dataset[t][i][j][1]) + '\n')
            grid_id = grid_id + 1
        for t in range(test_time):
            grid_file.write(str(grid_id) + ',' + 'state' + ',' + str(
                util.timestamp_datetime(test_start_timestamp + t * 1800)) + ',' + str(i) + ',' + str(j) + ',' + str(
                volume_test_dataset[t][i][j][0]) + ',' + str(volume_test_dataset[t][i][j][1]) + '\n')
            grid_id = grid_id + 1
grid_file.close()

gridod_id = 0
gridod_file = open(dataname + '.gridod', 'w')
gridod_file.write(
    'dyna_id' + ',' + 'type' + ',' + 'time' + ',' + 'origin_row_id' + ',' + 'origin_column_id' + ',' +
    'destination_row_id' + ',' + 'destination_column_id' + ',' + 'flow_from_cur' + ',' + 'flow_from_last' + '\n')
flow_train_dataset = np.load(open(dataurl + "bike_flow_train.npz", "rb"))["flow"]
flow_test_dataset = np.load(open(dataurl + "bike_flow_test.npz", "rb"))["flow"]
train_time = flow_train_dataset.shape[1]
test_time = flow_test_dataset.shape[1]
train_start_timestamp = util.datetime_timestamp("2016-07-01T00:00:00Z")
test_start_timestamp = util.datetime_timestamp("2016-08-10T00:00:00Z")
x = flow_train_dataset.shape[2]
y = flow_train_dataset.shape[3]
for origin_row_id in range(x):
    for origin_column_id in range(y):
        for destination_row_id in range(x):
            for destination_column_id in range(y):
                for t in range(train_time):
                    gridod_file.write(str(gridod_id) + ',' + 'state' + ',' + str(
                        util.timestamp_datetime(train_start_timestamp + t * 1800)) + ',' + str(
                        origin_row_id) + ',' + str(
                        origin_column_id) + ',' + str(destination_row_id) + ',' + str(
                        destination_column_id) + ',' + str(
                        flow_train_dataset[0][t][origin_row_id][origin_column_id][destination_row_id][
                            destination_column_id]) + ',' + str(
                        flow_train_dataset[1][t][origin_row_id][origin_column_id][destination_row_id][
                            destination_column_id]) + '\n')
                    gridod_id = gridod_id + 1
                for t in range(test_time):
                    gridod_file.write(str(gridod_id) + ',' + 'state' + ',' + str(
                        util.timestamp_datetime(test_start_timestamp + t * 1800)) + ',' + str(
                        origin_row_id) + ',' + str(
                        origin_column_id) + ',' + str(destination_row_id) + ',' + str(
                        destination_column_id) + ',' + str(
                        flow_test_dataset[0][t][origin_row_id][origin_column_id][destination_row_id][
                            destination_column_id]) + ',' + str(
                        flow_test_dataset[1][t][origin_row_id][origin_column_id][destination_row_id][
                            destination_column_id]) + '\n')
                    gridod_id = gridod_id + 1
gridod_file.close()

config = dict()
config['geo'] = dict()
config['geo']['including_types'] = ['Polygon']
config['geo']['Polygon'] = {'row_id': 'num', 'column_id': 'num'}
config['grid'] = dict()
config['grid']['including_types'] = ['state']
config['grid']['state'] = {'row_id': x, 'column_id': y, 'departing_volume': 'num', 'arriving_volume': 'num'}
config['gridod'] = dict()
config['gridod']['including_types'] = ['state']
config['gridod']['state'] = {'origin_row_id': x, 'origin_column_id': y, 'destination_row_id': x,
                             'destination_column_id': y, 'flow_from_cur': 'num', 'flow_from_last': 'num'}
config['info'] = dict()
config['info']['data_col'] = ['departing_volume', 'arriving_volume']
config['info']['data_files'] = ['NYCBike20160708']
config['info']['geo_file'] = 'NYCBike20160708'
config['info']['output_dim'] = 2
config['info']['time_intervals'] = 1800
config['info']['init_weight_inf_or_zero'] = 'inf'
config['info']['set_weight_link_or_dist'] = 'dist'
config['info']['calculate_weight_adj'] = False
config['info']['weight_adj_epsilon'] = 0.1
json.dump(config, open(outputdir + '/config.json', 'w', encoding='utf-8'), ensure_ascii=False)
