import numpy as np
import pandas as pd
import json
import util

outputdir = 'output/SZ-TAXI'
util.ensure_dir(outputdir)

dataurl = 'input/SZ-TAXI/'
dataname = outputdir+'/SZ-TAXI'

geo_id_list = pd.read_csv(dataurl+'sz_speed.csv',header=None,nrows=1)
geo_id_list = np.array(geo_id_list)[0]

geo = []
for i in range(len(geo_id_list)):
    geo.append([geo_id_list[i], 'LineString', '[[]]'])
geo = pd.DataFrame(geo, columns=['geo_id', 'type', 'coordinates'])
geo.to_csv(dataname+'.geo', index=False)

sz_adj = pd.read_csv(dataurl+'sz_adj.csv',header=None)
adj = np.mat(sz_adj)
rel = []
rel_id_counter = 0
for i in range(adj.shape[0]):
    for j in range(adj.shape[1]):
        rel.append([rel_id_counter, 'geo', geo_id_list[i], geo_id_list[j], adj[i,j]])
        rel_id_counter += 1
rel = pd.DataFrame(rel, columns=['rel_id', 'type', 'origin_id', 'destination_id', 'link_weight'])
rel.to_csv(dataname + '.rel', index=False)

sz_speed = pd.read_csv(dataurl+'sz_speed.csv')
speed = np.mat(sz_speed)
dyna = []
dyna_id_counter = 0


def num2time(num):
    day = (num) // 96 + 1
    hour = (num % 96) // 4
    quarter = num % 4

    day = str(day) if day > 9 else '0'+ str(day)
    hour = str(hour) if hour > 9 else '0' + str(hour)
    minute = str(15*quarter) if 15*quarter > 9 else '0' + str(15*quarter)

    time = '2015-01-' + day + 'T' + hour + ':' + minute + ':' + '00Z'
    return time


for j in range(speed.shape[1]):
    for i in range(speed.shape[0]):
        time = num2time(i)

        #               dyna_id,      type,    time, entity_id,     traffic_speed
        dyna.append([dyna_id_counter, 'state',  time, geo_id_list[j], speed[i, j]])
        dyna_id_counter += 1
dyna = pd.DataFrame(dyna, columns=['dyna_id', 'type', 'time', 'entity_id', 'traffic_speed'])
dyna.to_csv(dataname + '.dyna', index=False)


config = dict()
config['geo'] = dict()
config['geo']['including_types'] = ['LineString']
config['geo']['LineString'] = {}
config['rel'] = dict()
config['rel']['including_types'] = ['geo']
config['rel']['geo'] ={'link_weight': 'num'}
config['dyna'] = dict()
config['dyna']['including_types'] = ['state']
config['dyna']['state'] = {'entity_id': 'geo_id', 'traffic_speed': 'num'}
config['info'] = dict()
config['info']['data_col'] = ['traffic_speed']
config['info']['weight_col'] = 'link_weight'
config['info']['data_files'] = ['SZ-TAXI']
config['info']['geo_file'] = 'SZ-TAXI'
config['info']['rel_file'] = 'SZ-TAXI'
json.dump(config, open(outputdir+'/config.json', 'w', encoding='utf-8'), ensure_ascii=False)
