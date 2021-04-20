import h5py
import pandas as pd
import numpy as np
import json
import util

outputdir = 'output/NYCTAXI20140112'
util.ensure_dir(outputdir)

dataurl = 'input/NYCTAXI20140112/'
dataname = outputdir+'/NYCTAXI20140112'

f = h5py.File(dataurl + 'NYC2014.h5', 'r')
date_df = pd.DataFrame(np.array(f['date']))
data = np.array(f['data'])


def get_geo():
    li = []
    ind = 0
    for x in range(15):
        for y in range(5):
            li.append([ind, "Polygon", "[]", x, y])
            ind += 1
    return li


def remove_imcomplete_days(data, timestamps, t=48):
    print("before removing", len(data))
    date = []
    days = []
    days_incomplete = []
    i = 0
    print(len(timestamps))
    while i < len(timestamps):
        if int(str(timestamps[i])[10:12]) != 1:
            i += 1
        elif i + t - 1 < len(timestamps) \
                and int(str(timestamps[i + t - 1])[10:12]) == t:
            for j in range(48):
                date.append(timestamps[i + j])
            days.append(str(timestamps[i])[2:10])
            i += t
        else:
            days_incomplete.append(str(timestamps[i])[2:10])
            i += 1
    print("imcomplete days", days_incomplete)
    days = set(days)
    idx = []
    for i, t in enumerate(timestamps):
        if str(timestamps[i])[2:10] in days:
            idx.append(i)
    data_ = data[idx]
    print(len(date))
    print(len(data_))
    return date, data_


def del_date(date):
    date_str = str(date)
    s0 = date_str[2:6]
    s1 = date_str[6:8]
    s2 = date_str[8:10]
    s3 = date_str[10:12]
    num_s3 = int(s3) - 1
    num = 0
    if num_s3 % 2 == 0:
        num = int(num_s3 * 0.5)
        if num < 10:
            str_s3 = '0' + str(num)
        else:
            str_s3 = str(num)
        s = s0 + '-' + s1 + '-' + s2 + 'T' + str_s3 + ':00:00' + 'Z'
    else:
        num = (num_s3 - 1) * 0.5
        num = int(num)
        if num < 10:
            str_s3 = '0' + str(num)
        else:
            str_s3 = str(num)
        s = s0 + '-' + s1 + '-' + s2 + 'T' + str_s3 + ':30:00' + 'Z'

    return s


new_date, new_data = remove_imcomplete_days(np.array(f['data']),
                                            np.array(f['date']))
date_df = pd.DataFrame(new_date)
date_df['time'] = date_df[0].apply(del_date)


def get_dyna():
    ind = 0
    li = []
    for x in range(15):
        for y in range(5):
            for time in range(len(date_df['time'])):
                li.append([ind, "state", date_df['time'][time], x, y,
                          new_data[time][0][x][y], new_data[time][1][x][y]])
                ind += 1
    return li


L0 = get_geo()
pd.DataFrame(L0, columns=["geo_id", "type", "coordinates", "row_id",
                          "column_id"]).to_csv(dataname + '.geo', index=None)

L1 = get_dyna()
pd.DataFrame(L1, columns=["dyna_id", "type", "time", "row_id",
                          "column_id", "pickup", "dropoff"])\
    .to_csv(dataname + '.grid', index=None)


ext = h5py.File(dataurl + 'Meteorology.h5', 'r')
date = np.array(ext['date'])
Temperature = np.array(ext['Temperature'])
Weather = np.array(ext['Weather'])
WindSpeed = np.array(ext['WindSpeed'])
datenew = []

for da in date:
    datenew.append(del_date(da))
ext_id = np.array(range(len(datenew)))

extdf = pd.DataFrame()
extdf['ext_id'] = ext_id
extdf['time'] = datenew
extdf['Temperature'] = Temperature
extdf['WindSpeed'] = WindSpeed
columns = ['Temperature', 'WindSpeed']
for i in range(Weather.shape[1]):
    extdf['Weather'+str(i)] = Weather[:, i]
    columns.append('Weather' + str(i))
extdf.to_csv(dataname + '.ext', index=False)

config = dict()
config['geo'] = dict()
config['geo']['including_types'] = ['Polygon']
config['geo']['Polygon'] = {"row_id": 'num', "column_id": 'num'}
config['grid'] = dict()
config['grid']['including_types'] = ['state']
config['grid']['state'] = {'row_id': 15, 'column_id': 5,
                           "pickup": 'num', "dropoff": 'num'}
config['info'] = dict()
config['info']['data_col'] = ['pickup', 'dropoff']
config['info']['ext_col'] = columns
config['info']['data_files'] = ['NYCTAXI20140112']
config['info']['geo_file'] = 'NYCTAXI20140112'
config['info']['ext_file'] = 'NYCTAXI20140112'
config['info']['output_dim'] = 2
config['info']['time_intervals'] = 1800
config['info']['init_weight_inf_or_zero'] = 'inf'
config['info']['set_weight_link_or_dist'] = 'dist'
config['info']['calculate_weight_adj'] = False
config['info']['weight_adj_epsilon'] = 0.1
json.dump(config, open(outputdir + '/config.json', 'w',
                       encoding='utf-8'), ensure_ascii=False)
