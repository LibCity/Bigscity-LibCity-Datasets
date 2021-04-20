import pandas as pd
import json
import util

outputdir10 = 'output/BEIJING SUBWAY/BEIJING SUBWAY_10MIN'
outputdir15 = 'output/BEIJING SUBWAY/BEIJING SUBWAY_15MIN'
outputdir30 = 'output/BEIJING SUBWAY/BEIJING SUBWAY_30MIN'
util.ensure_dir(outputdir10)
util.ensure_dir(outputdir15)
util.ensure_dir(outputdir30)

dataurl = 'input/BEIJING SUBWAY/'
dataname10 = outputdir10 + '/BEIJING SUBWAY_10MIN'
dataname15 = outputdir15 + '/BEIJING SUBWAY_15MIN'
dataname30 = outputdir30 + '/BEIJING SUBWAY_30MIN'

weekdays = [
    (2, 29), (3, 1), (3, 2), (3, 3), (3, 4),
    (3, 7), (3, 8), (3, 9), (3, 10), (3, 11),
    (3, 14), (3, 15), (3, 16), (3, 17), (3, 18),
    (3, 21), (3, 22), (3, 23), (3, 24), (3, 25),
    (3, 28), (3, 29), (3, 30), (3, 31), (4, 1)
]


def id_to_time_10min(id):
    global weekdays
    year = 2016
    day_index = int(id / (18 * 6))
    month = weekdays[day_index][0]
    day = weekdays[day_index][1]
    hour_index = int((id - 18 * 6 * day_index) / 6)
    hour = 5 + hour_index
    min = int(id - 18 * 6 * day_index - 6 * hour_index) * 10
    sec = 0
    return str(year).zfill(4) + "-" + str(month).zfill(2) + "-" + str(day).zfill(2) + \
        "T" + str(hour).zfill(2) + ":" + str(min).zfill(2) + ":" + str(sec).zfill(2) + "Z"


def id_to_time_15min(id):
    global weekdays
    year = 2016
    day_index = int(id / (18 * 4))
    month = weekdays[day_index][0]
    day = weekdays[day_index][1]
    hour_index = int((id - 18 * 4 * day_index) / 4)
    hour = 5 + hour_index
    min = int(id - 18 * 4 * day_index - 4 * hour_index) * 15
    sec = 0
    return str(year).zfill(4) + "-" + str(month).zfill(2) + "-" + str(day).zfill(2) + \
        "T" + str(hour).zfill(2) + ":" + str(min).zfill(2) + ":" + str(sec).zfill(2) + "Z"


def id_to_time_30min(id):
    global weekdays
    year = 2016
    day_index = int(id / (18 * 2))
    month = weekdays[day_index][0]
    day = weekdays[day_index][1]
    hour_index = int((id - 18 * 2 * day_index) / 2)
    hour = 5 + hour_index
    min = int(id - 18 * 2 * day_index - 2 * hour_index) * 30
    sec = 0
    return str(year).zfill(4) + "-" + str(month).zfill(2) + "-" + str(day).zfill(2) + \
        "T" + str(hour).zfill(2) + ":" + str(min).zfill(2) + ":" + str(sec).zfill(2) + "Z"


in_10min_dataset = pd.read_csv(dataurl + 'data/inflowdata/in_10min.csv', header=None)
out_10min_dataset = pd.read_csv(dataurl + 'data/outflowdata/out_10min.csv', header=None)
in_15min_dataset = pd.read_csv(dataurl + 'data/inflowdata/in_15min.csv', header=None)
out_15min_dataset = pd.read_csv(dataurl + 'data/outflowdata/out_15min.csv', header=None)
in_30min_dataset = pd.read_csv(dataurl + 'data/inflowdata/in_15min.csv', header=None)
out_30min_dataset = pd.read_csv(dataurl + 'data/outflowdata/out_15min.csv', header=None)
station_num = in_10min_dataset.shape[0]
geo = []
for i in range(station_num):
    id = i
    geo.append([id, 'Point', '[]'])
geo = pd.DataFrame(geo, columns=['geo_id', 'type', 'coordinates'])
geo.to_csv(dataname10 + '.geo', index=False)
geo.to_csv(dataname15 + '.geo', index=False)
geo.to_csv(dataname30 + '.geo', index=False)


rel = []
reldict = dict()
rel_dataset = pd.read_csv(dataurl + 'adjacency.csv', header=None)
for i in range(station_num):
    sid = i
    for j in range(station_num):
        eid = j
        if (sid, eid) not in reldict:
            reldict[(sid, eid)] = len(reldict)
            rel.append([len(reldict) - 1, 'geo', sid, eid, rel_dataset[i][j]])
rel = pd.DataFrame(rel, columns=['rel_id', 'type', 'origin_id', 'destination_id', 'link_weight'])
rel.to_csv(dataname10 + '.rel', index=False)
rel.to_csv(dataname15 + '.rel', index=False)
rel.to_csv(dataname30 + '.rel', index=False)


dyna_id = 0
dyna_file = open(dataname10 + '.dyna', 'w')
dyna_file.write('dyna_id' + ',' + 'type' + ',' + 'time' + ',' + 'entity_id' + ',' +
                'in_flow' + ',' + 'out_flow' + '\n')
time_num_10min = int(25 * (23 - 5) * (60 / 10))
for i in range(station_num):
    entity_id = i
    for j in range(time_num_10min):
        time = id_to_time_10min(j)
        in_flow = in_10min_dataset[j][i]
        out_flow = out_10min_dataset[j][i]
        dyna_file.write(str(dyna_id) + ',' + 'state' + ',' + str(time) + ',' + str(entity_id) + ',' +
                        str(in_flow) + ',' + str(out_flow) + '\n')
        dyna_id += 1
        if dyna_id % 10000 == 0:
            print(str(dyna_id) + '/' + str(station_num * time_num_10min))
dyna_file.close()

dyna_id = 0
dyna_file = open(dataname15 + '.dyna', 'w')
dyna_file.write('dyna_id' + ',' + 'type' + ',' + 'time' + ',' + 'entity_id' + ',' +
                'in_flow' + ',' + 'out_flow' + '\n')
time_num_15min = int(25 * (23 - 5) * (60 / 15))
for i in range(station_num):
    entity_id = i
    for j in range(time_num_15min):
        time = id_to_time_15min(j)
        in_flow = in_15min_dataset[j][i]
        out_flow = out_15min_dataset[j][i]
        dyna_file.write(str(dyna_id) + ',' + 'state' + ',' + str(time) + ',' + str(entity_id) + ',' +
                        str(in_flow) + ',' + str(out_flow) + '\n')
        dyna_id += 1
        if dyna_id % 10000 == 0:
            print(str(dyna_id) + '/' + str(station_num * time_num_15min))
dyna_file.close()

dyna_id = 0
dyna_file = open(dataname30 + '.dyna', 'w')
dyna_file.write('dyna_id' + ',' + 'type' + ',' + 'time' + ',' + 'entity_id' + ',' +
                'in_flow' + ',' + 'out_flow' + '\n')
time_num_30min = int(25 * (23 - 5) * (60 / 30))
for i in range(station_num):
    entity_id = i
    for j in range(time_num_30min):
        time = id_to_time_30min(j)
        in_flow = in_30min_dataset[j][i]
        out_flow = out_30min_dataset[j][i]
        dyna_file.write(str(dyna_id) + ',' + 'state' + ',' + str(time) + ',' + str(entity_id) + ',' +
                        str(in_flow) + ',' + str(out_flow) + '\n')
        dyna_id += 1
        if dyna_id % 10000 == 0:
            print(str(dyna_id) + '/' + str(station_num * time_num_30min))
dyna_file.close()


meteo_dataset = pd.read_csv(dataurl + 'data/meteorology/10 min after normolization.csv', header=None)
ext_id = 0
ext_file = open(dataname10 + '.ext', 'w')
ext_file.write('ext_id' + ',' + 'time' + ',' +
               'temperature' + ',' + 'dew_point_temperature' + ',' +
               'relative_humidity' + ',' + 'wind_speed' + ',' +
               'aqi' + ',' + 'pm2.5' + ',' + 'pm10' + ',' +
               'so2' + ',' + 'no2' + ',' + 'co' + ',' +
               'o3' + '\n')
for j in range(time_num_10min):
    time = id_to_time_10min(j)
    temperature = meteo_dataset[j][0]
    dew_point_temperature = meteo_dataset[j][1]
    relative_humidity = meteo_dataset[j][2]
    wind_speed = meteo_dataset[j][3]
    aqi = meteo_dataset[j][4]
    pm2_5 = meteo_dataset[j][5]
    pm10 = meteo_dataset[j][6]
    so2 = meteo_dataset[j][7]
    no2 = meteo_dataset[j][8]
    co = meteo_dataset[j][9]
    o3 = meteo_dataset[j][10]
    ext_file.write(str(ext_id) + ',' + str(time) + ',' +
                   str(temperature) + ',' + str(dew_point_temperature) + ',' +
                   str(relative_humidity) + ',' + str(wind_speed) + ',' +
                   str(aqi) + ',' + str(pm2_5) + ',' + str(pm10) + ',' +
                   str(so2) + ',' + str(no2) + ',' + str(co) + ',' +
                   str(o3) + '\n')
    ext_id += 1
ext_file.close()

meteo_dataset = pd.read_csv(dataurl + 'data/meteorology/15 min after normolization.csv', header=None)
ext_id = 0
ext_file = open(dataname15 + '.ext', 'w')
ext_file.write('ext_id' + ',' + 'time' + ',' +
               'temperature' + ',' + 'dew_point_temperature' + ',' +
               'relative_humidity' + ',' + 'wind_speed' + ',' +
               'aqi' + ',' + 'pm2.5' + ',' + 'pm10' + ',' +
               'so2' + ',' + 'no2' + ',' + 'co' + ',' +
               'o3' + '\n')
for j in range(time_num_15min):
    time = id_to_time_15min(j)
    temperature = meteo_dataset[j][0]
    dew_point_temperature = meteo_dataset[j][1]
    relative_humidity = meteo_dataset[j][2]
    wind_speed = meteo_dataset[j][3]
    aqi = meteo_dataset[j][4]
    pm2_5 = meteo_dataset[j][5]
    pm10 = meteo_dataset[j][6]
    so2 = meteo_dataset[j][7]
    no2 = meteo_dataset[j][8]
    co = meteo_dataset[j][9]
    o3 = meteo_dataset[j][10]
    ext_file.write(str(ext_id) + ',' + str(time) + ',' +
                   str(temperature) + ',' + str(dew_point_temperature) + ',' +
                   str(relative_humidity) + ',' + str(wind_speed) + ',' +
                   str(aqi) + ',' + str(pm2_5) + ',' + str(pm10) + ',' +
                   str(so2) + ',' + str(no2) + ',' + str(co) + ',' +
                   str(o3) + '\n')
    ext_id += 1
ext_file.close()

meteo_dataset = pd.read_csv(dataurl + 'data/meteorology/30 min after normolization.csv', header=None)
ext_id = 0
ext_file = open(dataname30 + '.ext', 'w')
ext_file.write('ext_id' + ',' + 'time' + ',' +
               'temperature' + ',' + 'dew_point_temperature' + ',' +
               'relative_humidity' + ',' + 'wind_speed' + ',' +
               'aqi' + ',' + 'pm2.5' + ',' + 'pm10' + ',' +
               'so2' + ',' + 'no2' + ',' + 'co' + ',' +
               'o3' + '\n')
for j in range(time_num_30min):
    time = id_to_time_30min(j)
    temperature = meteo_dataset[j][0]
    dew_point_temperature = meteo_dataset[j][1]
    relative_humidity = meteo_dataset[j][2]
    wind_speed = meteo_dataset[j][3]
    aqi = meteo_dataset[j][4]
    pm2_5 = meteo_dataset[j][5]
    pm10 = meteo_dataset[j][6]
    so2 = meteo_dataset[j][7]
    no2 = meteo_dataset[j][8]
    co = meteo_dataset[j][9]
    o3 = meteo_dataset[j][10]
    ext_file.write(str(ext_id) + ',' + str(time) + ',' +
                   str(temperature) + ',' + str(dew_point_temperature) + ',' +
                   str(relative_humidity) + ',' + str(wind_speed) + ',' +
                   str(aqi) + ',' + str(pm2_5) + ',' + str(pm10) + ',' +
                   str(so2) + ',' + str(no2) + ',' + str(co) + ',' +
                   str(o3) + '\n')
    ext_id += 1
ext_file.close()


config = dict()
config['geo'] = dict()
config['geo']['including_types'] = ['Point']
config['geo']['Point'] = {}
config['rel'] = dict()
config['rel']['including_types'] = ['geo']
config['rel']['geo'] = {'link_weight': 'num'}
config['dyna'] = dict()
config['dyna']['including_types'] = ['state']
config['dyna']['state'] = {'entity_id': 'geo_id', 'in_flow': 'num', 'out_flow': 'num'}
config['ext'] = {
    'ext_id': 'num', 'time': 'other',
    'temperature': 'num', 'dew_point_temperature': 'num',
    'relative_humidity': 'num', 'wind_speed': 'num',
    'aqi': 'num', 'pm2.5': 'num', 'pm10': 'num',
    'so2': 'num', 'no2': 'num', 'co': 'num',
    'o3': 'num'
}
config['info'] = dict()
config['info']['data_col'] = ['in_flow', 'out_flow']
config['info']['weight_col'] = 'link_weight'
config['info']['ext_col'] = ['temperature', 'dew_point_temperature', 'relative_humidity', 'wind_speed',
                             'aqi', 'pm2.5', 'pm10', 'so2', 'no2', 'co', 'o3']
config['info']['data_files'] = ['BEIJING SUBWAY_10MIN']
config['info']['geo_file'] = 'BEIJING SUBWAY_10MIN'
config['info']['rel_file'] = 'BEIJING SUBWAY_10MIN'
config['info']['ext_file'] = 'BEIJING SUBWAY_10MIN'
config['info']['output_dim'] = 2
config['info']['time_intervals'] = 600
config['info']['init_weight_inf_or_zero'] = 'inf'
config['info']['set_weight_link_or_dist'] = 'dist'
config['info']['calculate_weight_adj'] = False
config['info']['weight_adj_epsilon'] = 0.1
json.dump(config, open(outputdir10 + '/config.json', 'w', encoding='utf-8'), ensure_ascii=False)

config['info']['data_files'] = ['BEIJING SUBWAY_15MIN']
config['info']['geo_file'] = 'BEIJING SUBWAY_15MIN'
config['info']['rel_file'] = 'BEIJING SUBWAY_15MIN'
config['info']['ext_file'] = 'BEIJING SUBWAY_15MIN'
config['info']['time_intervals'] = 900
json.dump(config, open(outputdir15 + '/config.json', 'w', encoding='utf-8'), ensure_ascii=False)

config['info']['data_files'] = ['BEIJING SUBWAY_30MIN']
config['info']['geo_file'] = 'BEIJING SUBWAY_30MIN'
config['info']['rel_file'] = 'BEIJING SUBWAY_30MIN'
config['info']['ext_file'] = 'BEIJING SUBWAY_30MIN'
config['info']['time_intervals'] = 1800
json.dump(config, open(outputdir30 + '/config.json', 'w', encoding='utf-8'), ensure_ascii=False)
