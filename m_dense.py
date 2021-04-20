import pandas as pd
import json
import util

outputdir = 'output/M_DENSE'
util.ensure_dir(outputdir)

dataurl = 'input/M_DENSE/'
dataname = outputdir+'/M_DENSE'


month_to_day_num = [
    31, 59, 90, 120,
    151, 181, 212, 243,
    273, 304, 334, 365
]


def id_to_time(id):
    global month_to_day_num
    year_index = int(id / (365 * 24))
    year = 2018 + year_index
    year_day_index = int((id - 365 * 24 * year_index) / 24)
    month_index = 0
    for i in range(12):
        if month_to_day_num[i] > year_day_index:
            month_index = i
            break
    month = 1 + month_index
    day_index = year_day_index - month_to_day_num[month_index-1] if month_index > 0 else year_day_index
    day = 1 + day_index
    hour = int(id - 365 * 24 * year_index - 24 * year_day_index)
    min = 0
    sec = 0
    return str(year).zfill(4) + "-" + str(month).zfill(2) + "-" + str(day).zfill(2) + \
        "T" + str(hour).zfill(2) + ":" + str(min).zfill(2) + ":" + str(sec).zfill(2) + "Z"


old_dataset = pd.read_csv(dataurl+'data/dense_data.csv', header=None)
sensor_num = 30
timeid_num = old_dataset.shape[0]
dataset = []
for i in range(timeid_num):
    datas = old_dataset[0][i].split(' ')
    datas = [eval(data) for data in datas]
    dataset.append(datas)


geo = []
for i in range(sensor_num):
    id = i
    geo.append([id, 'Point', '[]'])
geo = pd.DataFrame(geo, columns=['geo_id', 'type', 'coordinates'])
geo.to_csv(dataname + '.geo', index=False)


dyna_id = 0
dyna_file = open(dataname+'.dyna', 'w')
dyna_file.write('dyna_id' + ',' + 'type' + ',' + 'time' + ',' + 'entity_id' + ',' + 'traffic_intensity' + '\n')
for j in range(sensor_num):
    entity_id = j
    for i in range(timeid_num):
        time = id_to_time(i)
        dyna_file.write(str(dyna_id) + ',' + 'state' + ',' + str(time)
                        + ',' + str(entity_id) + ',' + str(dataset[i][j]) + '\n')
        dyna_id = dyna_id + 1
        if dyna_id % 10000 == 0:
            print(str(dyna_id) + '/' + str(sensor_num * timeid_num))
dyna_file.close()


ext_id = 0
ext_file = open(dataname + '.ext', 'w')
ext_file.write('ext_id' + ',' + 'time' + ',' +
               'mean' + ',' + 'wind_speed' + ',' +
               'temperature' + ',' + 'humidity' + ',' +
               'pressure' + ',' + 'radiation' + ',' +
               'precipitation' + '\n')
for i in range(timeid_num):
    time = id_to_time(i)
    mean = dataset[i][sensor_num + 0]
    wind_speed = dataset[i][sensor_num + 1]
    temperature = dataset[i][sensor_num + 2]
    humidity = dataset[i][sensor_num + 3]
    pressure = dataset[i][sensor_num + 4]
    radiation = dataset[i][sensor_num + 5]
    precipitation = dataset[i][sensor_num + 6]
    ext_file.write(str(ext_id) + ',' + str(time) + ',' +
                   str(mean) + ',' + str(wind_speed) + ',' +
                   str(temperature) + ',' + str(humidity) + ',' +
                   str(pressure) + ',' + str(radiation) + ',' +
                   str(precipitation) + '\n')
    ext_id += 1
ext_file.close()


config = dict()
config['geo'] = dict()
config['geo']['including_types'] = ['Point']
config['geo']['Point'] = {}
config['dyna'] = dict()
config['dyna']['including_types'] = ['state']
config['dyna']['state'] = {'entity_id': 'geo_id', 'traffic_intensity': 'num'}
config['ext'] = {
    'ext_id': 'num', 'time': 'other',
    'mean': 'num', 'wind_speed': 'num',
    'temperature': 'num', 'humidity': 'num',
    'pressure': 'num', 'radiation': 'num',
    'precipitation': 'num'
}
config['info'] = dict()
config['info']['data_col'] = 'traffic_intensity'
config['info']['ext_col'] = ['mean', 'wind_speed', 'temperature', 'humidity',
                             'pressure', 'radiation', 'precipitation']
config['info']['data_files'] = ['M_DENSE']
config['info']['geo_file'] = 'M_DENSE'
config['info']['ext_file'] = 'M_DENSE'
config['info']['output_dim'] = 1
config['info']['time_intervals'] = 3600
config['info']['init_weight_inf_or_zero'] = 'inf'
config['info']['set_weight_link_or_dist'] = 'dist'
config['info']['calculate_weight_adj'] = False
config['info']['weight_adj_epsilon'] = 0.1
json.dump(config, open(outputdir+'/config.json', 'w', encoding='utf-8'), ensure_ascii=False)
