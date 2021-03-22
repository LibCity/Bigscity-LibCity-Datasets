import numpy as np
import pandas as pd
import json
import util

outputdir = 'output/NYCBIKE20160809'
util.ensure_dir(outputdir)

dataurl = 'input/NYCBIKE20160809/'
dataname = outputdir+'/NYCBIKE20160809'

data_train = np.load(dataurl + 'bike_train.npz')
data_train_flow = data_train['flow']
data_test = np.load(dataurl + 'bike_test.npz')
data_test_flow = data_test['flow']


def get_geo():
    li = []
    ind = 0
    for x in range(14):
        for y in range(8):
            li.append([ind, "Polygon", "[]", x, y])
            ind += 1
    return li


def get_time(x):
    if x % 2 == 0:
        x = int(x*0.5)
        y = int((x - x % 24) / 24 + 1)
        if y > 31:
            month = 9
            day = y - 31
            if day < 10:
                day_str = '0'+str(day)
            else:
                day_str = str(day)
        else:
            month = 8
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

        return '2016-0' + str(month) + '-' + \
               day_str + 'T' + hour_str + ':00:00Z'
    else:
        x = x - 1
        x = int(x * 0.5)
        y = int((x - x % 24) / 24 + 1)
        if y > 31:
            month = 9
            day = y - 31
            if day < 10:
                day_str = '0' + str(day)
            else:
                day_str = str(day)
        else:
            month = 8
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

        return '2016-0' + str(month) + '-' + \
               day_str + 'T' + hour_str + ':30:00Z'


def get_dyna():
    ind = 0
    l = []
    for x in range(14):
        for y in range(8):
            for time in range(1920):
                l.append([ind, "state", get_time(time), x, y,
                          data_train_flow[time][x][y][0],
                          data_train_flow[time][x][y][1]])
                ind += 1
            for time in range(960):
                l.append([ind, "state", get_time(time+1920), x, y,
                          data_test_flow[time][x][y][0],
                          data_test_flow[time][x][y][1]])
                ind += 1
    return l


L0 = get_geo()
pd.DataFrame(L0, columns=["geo_id", "type", "coordinates",
                          "row_id", "column_id"]).\
    to_csv(dataname + '.geo', index=None)
L1 = get_dyna()
pd.DataFrame(L1, columns=["dyna_id", "type", "time", "row_id",
                          "column_id", "inflow", "outflow"]).\
    to_csv(dataname + '.grid', index=None)


config = dict()
config['geo'] = dict()
config['geo']['including_types'] = ['Polygon']
config['geo']['Polygon'] = {"row_id": 'num', "column_id": 'num'}
config['grid'] = dict()
config['grid']['including_types'] = ['state']
config['grid']['state'] = {'row_id': 14, 'column_id': 8,
                           'inflow': 'num', 'outflow': 'num'}
config['info'] = dict()
config['info']['data_col'] = ['inflow', 'outflow']
config['info']['data_files'] = ['NYCBIKE20160809']
config['info']['geo_file'] = 'NYCBIKE20160809'
config['info']['output_dim'] = 2
config['info']['init_weight_inf_or_zero'] = 'inf'
config['info']['set_weight_link_or_dist'] = 'dist'
config['info']['calculate_weight_adj'] = False
config['info']['weight_adj_epsilon'] = 0.1
json.dump(config, open(outputdir + '/config.json',
                       'w', encoding='utf-8'), ensure_ascii=False)
