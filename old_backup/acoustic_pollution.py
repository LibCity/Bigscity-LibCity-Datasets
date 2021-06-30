import util
import pandas as pd
import numpy as np
import json

outputdir = 'output/AcousticPollution'
util.ensure_dir(outputdir)

dataurl = 'input/AcousticPollution/'
dataname = outputdir + '/AcousticPollution'

dataset = pd.read_csv(dataurl + "acpol.csv", sep=" ", header=None)
geo = []
for i in range(dataset.shape[1]):
    geo.append([i, 'Point', '[]'])
geo = pd.DataFrame(geo, columns=['geo_id', 'type', 'coordinates'])
geo.to_csv(dataname + '.geo', index=False)

dyna_id = 0
dyna_file = open(dataname + '.dyna', 'w')
dyna_file.write('dyna_id' + ',' + 'type' + ',' + 'time' + ',' + 'entity_id' + ',' + 'acoustic_pollution' + '\n')
for i in range(dataset.shape[1]):
    for j in range(dataset.shape[0]):
        dyna_file.write(
            str(dyna_id) + ',' + 'state' + ',' + str(np.nan) + ',' + str(i) + ',' + str(dataset[i][j]) + '\n')
dyna_file.close()

config = dict()
config['geo'] = dict()
config['geo']['including_types'] = ['Point']
config['geo']['Point'] = {}
config['dyna'] = dict()
config['dyna']['including_types'] = ['state']
config['dyna']['state'] = {'entity_id': 'geo_id', 'acoustic_pollution': 'num'}
config['info'] = dict()
config['info']['data_col'] = ['acoustic_pollution']
config['info']['data_files'] = ['AcousticPollution']
config['info']['geo_file'] = 'AcousticPollution'
config['info']['output_dim'] = 1
config['info']['time_intervals'] = None
config['info']['init_weight_inf_or_zero'] = 'inf'
config['info']['set_weight_link_or_dist'] = 'dist'
config['info']['calculate_weight_adj'] = False
config['info']['weight_adj_epsilon'] = 0.1
json.dump(config, open(outputdir + '/config.json', 'w', encoding='utf-8'), ensure_ascii=False)
