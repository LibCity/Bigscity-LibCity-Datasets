import h5py
import pandas as pd
import numpy as np
import json
import util

outputdir = 'output/TAXIBJ'
util.ensure_dir(outputdir)

dataurl = 'input/TAXIBJ/'
dataname1 = outputdir+'/TAXIBJ2013'
dataname2 = outputdir+'/TAXIBJ2014'
dataname3 = outputdir+'/TAXIBJ2015'
dataname4 = outputdir+'/TAXIBJ2016'

f1 = h5py.File(dataurl + 'BJ13_M32x32_T30_InOut.h5', 'r')
data1 = np.array(f1['data'])

f2 = h5py.File(dataurl + 'BJ14_M32x32_T30_InOut.h5', 'r')
data2 = np.array(f2['data'])

f3 = h5py.File(dataurl + 'BJ15_M32x32_T30_InOut.h5', 'r')
data3 = np.array(f3['data'])

f4 = h5py.File(dataurl + 'BJ16_M32x32_T30_InOut.h5',  'r')
data4 = np.array(f4['data'])


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
        elif i + t - 1 < len(timestamps) and \
                int(str(timestamps[i + t - 1])[10:12]) == t:
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


new_date1, new_data1 = remove_imcomplete_days(np.array(f1['data']),
                                              np.array(f1['date']))
date_df1 = pd.DataFrame(new_date1)
new_date2, new_data2 = remove_imcomplete_days(np.array(f2['data']),
                                              np.array(f2['date']))
date_df2 = pd.DataFrame(new_date2)
new_date3, new_data3 = remove_imcomplete_days(np.array(f3['data']),
                                              np.array(f3['date']))
date_df3 = pd.DataFrame(new_date3)
new_date4, new_data4 = remove_imcomplete_days(np.array(f4['data']),
                                              np.array(f4['date']))
date_df4 = pd.DataFrame(new_date4)


def get_geo():
    li = []
    ind = 0
    for x in range(32):
        for y in range(32):
            li.append([ind, "Polygon", "[]", x, y])
            ind += 1
    return li


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


date_df1['time'] = date_df1[0].apply(del_date)
date_df2['time'] = date_df2[0].apply(del_date)
date_df3['time'] = date_df3[0].apply(del_date)
date_df4['time'] = date_df4[0].apply(del_date)


def get_dyna1():
    ind = 0
    li = []
    for x in range(32):
        for y in range(32):
            for time in range(len(date_df1['time'])):
                li.append([ind, "state", date_df1['time'][time], x, y,
                          new_data1[time][0][x][y], new_data1[time][1][x][y]])
                ind += 1
    return li


def get_dyna2():
    ind = 0
    li = []
    for x in range(32):
        for y in range(32):
            for time in range(len(date_df2['time'])):
                li.append([ind, "state", date_df2['time'][time], x, y,
                          new_data2[time][0][x][y], new_data2[time][1][x][y]])
                ind += 1
    return li


def get_dyna3():
    ind = 0
    li = []
    for x in range(32):
        for y in range(32):
            for time in range(len(date_df3['time'])):
                li.append([ind, "state", date_df3['time'][time], x, y,
                          new_data3[time][0][x][y], new_data3[time][1][x][y]])
                ind += 1
    return li


def get_dyna4():
    ind = 0
    li = []
    for x in range(32):
        for y in range(32):
            for time in range(len(date_df4['time'])):
                li.append([ind, "state", date_df4['time'][time], x, y,
                          new_data4[time][0][x][y], new_data4[time][1][x][y]])
                ind += 1
    return li


L_a = get_geo()
pd.DataFrame(L_a, columns=["geo_id", "type", "coordinates", "row_id",
                           "column_id"]).\
    to_csv(outputdir + '/TAXIBJ' + '.geo', index=None)


L_2013_b = get_dyna1()
pd.DataFrame(L_2013_b, columns=["dyna_id", "type", "time", "row_id",
                                "column_id", "inflow", "outflow"]).\
    to_csv(dataname1 + '.grid', index=None)
L_2014_b = get_dyna2()
pd.DataFrame(L_2014_b, columns=["dyna_id", "type", "time", "row_id",
                                "column_id", "inflow", "outflow"]).\
    to_csv(dataname2 + '.grid', index=None)
L_2015_b = get_dyna3()
pd.DataFrame(L_2015_b, columns=["dyna_id", "type", "time", "row_id",
                                "column_id", "inflow", "outflow"]).\
    to_csv(dataname3 + '.grid', index=None)
L_2016_b = get_dyna4()
pd.DataFrame(L_2016_b, columns=["dyna_id", "type", "time", "row_id",
                                "column_id", "inflow", "outflow"]).\
    to_csv(dataname4 + '.grid', index=None)


ext = h5py.File(dataurl + 'BJ_Meteorology.h5', 'r')
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
    columns.append('Weather'+str(i))
extdf.to_csv(outputdir + '/TAXIBJ.ext', index=False)


config = dict()
config['geo'] = dict()
config['geo']['including_types'] = ['Polygon']
config['geo']['Polygon'] = {"row_id": 'num', "column_id": 'num'}
config['grid'] = dict()
config['grid']['including_types'] = ['state']
config['grid']['state'] = {'row_id': 32, 'column_id': 32,
                           "inflow": 'num', "outflow": 'num'}
config['info'] = dict()
config['info']['data_col'] = ["inflow", "outflow"]
config['info']['ext_col'] = columns
config['info']['data_files'] = ["TAXIBJ2013",
                                "TAXIBJ2014", "TAXIBJ2015", "TAXIBJ2016"]
config['info']['geo_file'] = 'TAXIBJ'
config['info']['ext_file'] = 'TAXIBJ'
config['info']['output_dim'] = 2
config['info']['time_intervals'] = 1800
config['info']['init_weight_inf_or_zero'] = 'inf'
config['info']['set_weight_link_or_dist'] = 'dist'
config['info']['calculate_weight_adj'] = False
config['info']['weight_adj_epsilon'] = 0.1
json.dump(config, open(outputdir + '/config.json',
                       'w', encoding='utf-8'), ensure_ascii=False)
