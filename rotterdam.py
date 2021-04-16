import h5py
import numpy as np
import util
import pandas as pd
import time
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


outputdir = 'output/Rotterdam'
util.ensure_dir(outputdir)

dataurl = 'input/Rotterdam/'
dataname = outputdir + '/Rotterdam'

geo = []
for i in range(208):
    geo.append([i, 'Point', '[]'])
geo = pd.DataFrame(geo, columns=['geo_id', 'type', 'coordinates'])
geo.to_csv(dataname + '.geo', index=False)

obs_train = load_h5(dataurl + "RRot_cc2_20.h5", ["Speed_obs_train"])
pred_train = load_h5(dataurl + "RRot_cc2_20.h5", ["Speed_pred_train"])
obs_test = load_h5(dataurl + "RRot_cc2_20.h5", ["Speed_obs_test"])
pred_test = load_h5(dataurl + "RRot_cc2_20.h5", ["Speed_pred_test"])

dyna_id = 0
dyna_file = open(dataname + '.dyna', 'w')
dyna_file.write('dyna_id' + ',' + 'type' + ',' + 'time' + ',' + 'entity_id' + ',' + 'traffic_speed' + '\n')
test_new_day = [0]
for i in range(obs_test.shape[0] - 1):
    if obs_test[i, 1, 0, 0] != obs_test[i + 1, 0, 0, 0]:
        test_new_day.append(i + 1)
test_new_day.append(obs_test.shape[0])
for i in range(208):
    start_time = util.datetime_timestamp('2018-01-01T00:00:00Z')
    for j in range(110):
        # 跳过周末
        if time.localtime(start_time).tm_wday == 5:
            start_time = start_time + 2 * 24 * 3600
        day_time = start_time + 14 * 3600
        for t in range(150):
            dyna_file.write(
                str(dyna_id) + ',' + 'state' + ',' + str(util.timestamp_datetime(day_time)) + ',' + str(i) + ',' + str(
                    obs_train[j * 150 + t, 0, i, 0] * 120) + '\n')
            day_time += 120
            dyna_id += 1
        for t in range(135, 149):
            dyna_file.write(
                str(dyna_id) + ',' + 'state' + ',' + str(util.timestamp_datetime(day_time)) + ',' + str(i) + ',' + str(
                    pred_train[j * 150 + t, 0, i] * 120) + '\n')
            day_time += 120
            dyna_id += 1
        for t in range(10):
            dyna_file.write(
                str(dyna_id) + ',' + 'state' + ',' + str(util.timestamp_datetime(day_time)) + ',' + str(i) + ',' + str(
                    pred_train[j * 150 + 149, t, i] * 120) + '\n')
            day_time += 120
            dyna_id += 1
        start_time = start_time + 24 * 3600
    for j in range(25):
        # 跳过周末
        if time.localtime(start_time).tm_wday == 5:
            start_time += 2 * 24 * 3600
        day_time = start_time + 14 * 3600
        start_time = start_time + 24 * 3600
        if test_new_day[j + 1] - test_new_day[j] != 150:
            # 如果该天的数据量和其他天不对应，则跳过改天
            continue
        for t in range(test_new_day[j], test_new_day[j + 1]):
            dyna_file.write(
                str(dyna_id) + ',' + 'state' + ',' + str(util.timestamp_datetime(day_time)) + ',' + str(i) + ',' + str(
                    obs_test[t, 0, i, 0] * 120) + '\n')
            day_time += 120
            dyna_id += 1
        for t in range(test_new_day[j + 1] - 15, test_new_day[j + 1] - 1):
            dyna_file.write(
                str(dyna_id) + ',' + 'state' + ',' + str(util.timestamp_datetime(day_time)) + ',' + str(i) + ',' + str(
                    pred_test[t, 0, i] * 120) + '\n')
            day_time += 120
            dyna_id += 1
        for t in range(10):
            dyna_file.write(
                str(dyna_id) + ',' + 'state' + ',' + str(util.timestamp_datetime(day_time)) + ',' + str(i) + ',' + str(
                    pred_test[test_new_day[j + 1] - 1, t, i] * 120) + '\n')
            day_time += 120
            dyna_id += 1
dyna_file.close()

config = dict()
config['geo'] = dict()
config['geo']['including_types'] = ['Point']
config['geo']['Point'] = {}
config['dyna'] = dict()
config['dyna']['including_types'] = ['state']
config['dyna']['state'] = {'entity_id': 'geo_id', 'traffic_speed': 'num'}
config['info'] = dict()
config['info']['data_col'] = 'traffic_speed'
config['info']['data_files'] = ['Rotterdam']
config['info']['geo_file'] = 'Rotterdam'
config['info']['output_dim'] = 1
config['info']['time_intervals'] = 120
config['info']['init_weight_inf_or_zero'] = 'inf'
config['info']['set_weight_link_or_dist'] = 'dist'
config['info']['calculate_weight_adj'] = False
config['info']['weight_adj_epsilon'] = 0.1
json.dump(config, open(outputdir + '/config.json', 'w', encoding='utf-8'), ensure_ascii=False)
