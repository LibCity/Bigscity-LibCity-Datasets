import util
import numpy as np
import pandas as pd
import json

outputdir = 'output/HEAT'
util.ensure_dir(outputdir)

dataurl = 'input/HEAT/'
dataname = outputdir + '/HEAT'

idset = set()
geo = []
for i in range(41):
    geo.append([str(i), 'Point', '[]'])
geo = pd.DataFrame(geo, columns=['geo_id', 'type', 'coordinates'])
geo.to_csv(dataname + '.geo', index=False)

rel = []
rel_id = 0
dataset = np.genfromtxt(dataurl + "heat_relations.csv")
for i in range(41):
    for j in range(41):
        rel.append([rel_id, 'geo', i, j, dataset[i][j]])
        rel_id += 1
rel = pd.DataFrame(rel, columns=['rel_id', 'type', 'origin_id', 'destination_id', 'adj'])
rel.to_csv(dataname + '.rel', index=False)

dataset = np.genfromtxt(dataurl + "heat.csv").reshape(200, 41, 1)
dyna_id = 0
dyna_file = open(dataname + '.dyna', 'w')
dyna_file.write('dyna_id' + ',' + 'type' + ',' + 'time' + ',' + 'entity_id' + ',' + 'temperature' + '\n')
for i in range(41):
    for t in range(200):
        dyna_file.write(
            str(dyna_id) + ',' + 'state' + ',' + str(np.nan) + ',' + str(i) + ',' + str(dataset[t][i][0]) + '\n')
        dyna_id += 1
dyna_file.close()

dataset = np.genfromtxt(dataurl + "heat_m.csv").reshape(200, 41, 1)
dyna_id = 0
dyna_file = open(dataname + '_M.dyna', 'w')
dyna_file.write('dyna_id' + ',' + 'type' + ',' + 'time' + ',' + 'entity_id' + ',' + 'temperature' + '\n')
for i in range(41):
    for t in range(200):
        dyna_file.write(
            str(dyna_id) + ',' + 'state' + ',' + str(np.nan) + ',' + str(i) + ',' + str(dataset[t][i][0]) + '\n')
        dyna_id += 1
dyna_file.close()

config = dict()
config['geo'] = dict()
config['geo']['including_types'] = ['Point']
config['geo']['Point'] = {}
config['rel'] = dict()
config['rel']['including_types'] = ['geo']
config['rel']['geo'] = {'adj': 'num'}
config['dyna'] = dict()
config['dyna']['including_types'] = ['state']
config['dyna']['state'] = {'entity_id': 'geo_id', 'temperature': 'num'}
config['info'] = dict()
config['info']['data_col'] = 'temperature'
config['info']['weight_col'] = 'adj'
config['info']['data_files'] = ['HEAT', 'HEAT_M']
config['info']['geo_file'] = 'HEAT'
config['info']['rel_file'] = 'HEAT'
config['info']['output_dim'] = 1
config['info']['init_weight_inf_or_zero'] = 'inf'
config['info']['set_weight_link_or_dist'] = 'dist'
config['info']['calculate_weight_adj'] = False
config['info']['weight_adj_epsilon'] = 0.1
json.dump(config, open(outputdir + '/config.json', 'w', encoding='utf-8'), ensure_ascii=False)
