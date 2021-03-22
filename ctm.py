import numpy as np
import pandas as pd
import json
import util

outputdir = 'output/CTM'
util.ensure_dir(outputdir)

dataurl = 'input/CTM/'
dataname = outputdir+'/CTM'
data_train = np.load(dataurl + 'ctm_train.npz')
data_train_data = data_train['data']
data_test = np.load(dataurl + 'ctm_test.npz')
data_test_data = data_test['data']


def get_Geo():
    L = []
    ind = 0
    for x in range(20):
        for y in range(21):
            L.append([ind, "Polygon", "[]", x, y])
            ind += 1
    return L


def get_time(x):
    if x % 4 == 0:
        x = int(x * 0.25)
        y = int((x - x % 24) / 24 + 11)
        if y > 31:
            month = 11
            day = y - 31
            if day < 10:
                day_str = '0'+str(day)
            else:
                day_str = str(day)
        else:
            month = 10
            day = y
            if day < 10:
                day_str = '0'+str(day)
            else:
                day_str = str(day)
        hour = x % 24
        if hour < 10:
            hour_str = '0' + str(hour)
        else:
            hour_str = str(hour)

        return '2018-' + str(month) + '-' + day_str + \
               'T' + hour_str + ':00:00Z'
    elif x % 4 == 1:
        x = x - 1
        x = int(x * 0.25)
        y = int((x - x % 24) / 24 + 11)
        if y > 31:
            month = 11
            day = y - 31
            if day < 10:
                day_str = '0'+str(day)
            else:
                day_str = str(day)
        else:
            month = 10
            day = y
            if day < 10:
                day_str = '0'+str(day)
            else:
                day_str = str(day)
        hour = x % 24
        if hour < 10:
            hour_str = '0' + str(hour)
        else:
            hour_str = str(hour)

        return '2018-' + str(month) + '-' + day_str + 'T' \
               + hour_str + ':15:00Z'
    elif x % 4 == 2:
        x = x - 2
        x = int(x * 0.25)
        y = int((x - x % 24) / 24 + 11)
        if y > 31:
            month = 11
            day = y - 31
            if day < 10:
                day_str = '0'+str(day)
            else:
                day_str = str(day)
        else:
            month = 10
            day = y
            if day < 10:
                day_str = '0'+str(day)
            else:
                day_str = str(day)
        hour = x % 24
        if hour < 10:
            hour_str = '0' + str(hour)
        else:
            hour_str = str(hour)

        return '2018-' + str(month) + '-' + day_str \
               + 'T' + hour_str + ':30:00Z'
    else:
        x = x - 3
        x = int(x * 0.25)
        y = int((x - x % 24) / 24 + 11)
        if y > 31:
            month = 11
            day = y - 31
            if day < 10:
                day_str = '0' + str(day)
            else:
                day_str = str(day)
        else:
            month = 10
            day = y
            if day < 10:
                day_str = '0' + str(day)
            else:
                day_str = str(day)
        hour = x % 24
        if hour < 10:
            hour_str = '0' + str(hour)
        else:
            hour_str = str(hour)

        return '2018-' + str(month) + '-' + day_str \
               + 'T' + hour_str + ':45:00Z'


def get_Dyan():
    ind = 0
    L = []
    for x in range(20):
        for y in range(21):
            for time in range(2880):
                L.append([ind, "state", get_time(time),
                          x, y, data_train_data[time][x][y][0],
                          data_train_data[time][x][y][1]])
                ind += 1
            for time in range(1440):
                L.append([ind, "state", get_time(time + 2880),
                          x, y, data_test_data[time][x][y][0],
                          data_test_data[time][x][y][1]])
                ind += 1
    return L


L0 = get_Geo()
pd.DataFrame(L0, columns=["geo_id", "type", "coordinates", "row_id",
                          "column_id"]).\
    to_csv(dataname + '.geo', index=None)
print("finish geo")
L1 = get_Dyan()
pd.DataFrame(L1, columns=["dyna_id", "type", "time", "row_id",
                          "column_id", "duration", "request number"]).\
    to_csv(dataname + '.grid', index=None)
print("finish dyan")


config = dict()
config['geo'] = dict()
config['geo']['including_types'] = ['Polygon']
config['geo']['Polygon'] = {"row_id": 'num', "column_id": 'num'}
config['grid'] = dict()
config['grid']['including_types'] = ['state']
config['grid']['state'] = {'row_id': 20, 'column_id': 21,
                           'durations': 'num', 'request number': 'num'}
config['info'] = dict()
config['info']['data_col'] = ['durations', 'request number']
config['info']['data_files'] = ['CTM']
config['info']['geo_file'] = 'CTM'
config['info']['output_dim'] = 2
config['info']['init_weight_inf_or_zero'] = 'inf'
config['info']['set_weight_link_or_dist'] = 'dist'
config['info']['calculate_weight_adj'] = False
config['info']['weight_adj_epsilon'] = 0.1
json.dump(config, open(outputdir + '/config.json', 'w',
                       encoding='utf-8'), ensure_ascii=False)
