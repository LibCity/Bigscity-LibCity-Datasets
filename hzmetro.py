# link: https://github.com/ivechan/PVCGN
import util
import pickle
import pandas as pd
import json

outputdir = 'output/HZMETRO'
util.ensure_dir(outputdir)

dataurl = 'input/HZMetro/'
dataname = outputdir + '/HZMETRO'

data = {}
for category in ['train', 'val', 'test']:
    cat_data = pickle.load(open(dataurl + category + '.pkl', "rb"))
    data['x_' + category] = cat_data['x']
    data['xtime_' + category] = cat_data['xtime']
    data['y_' + category] = cat_data['y']
    data['ytime_' + category] = cat_data['ytime']
cor = pickle.load(open(dataurl + 'graph_hz_cor.pkl', "rb"))
conn = pickle.load(open(dataurl + 'graph_hz_conn.pkl', "rb"))
sml = pickle.load(open(dataurl + 'graph_hz_sml.pkl', "rb"))

geo = []
for i in range(conn.shape[0]):
    geo.append([i, 'Point', '[]'])
geo = pd.DataFrame(geo, columns=['geo_id', 'type', 'coordinates'])
geo.to_csv(dataname + '.geo', index=False)

rel = []
reldict = dict()
rel_id = 0
for i in range(conn.shape[0]):
    for j in range(conn.shape[1]):
        rel.append([rel_id, 'geo', i, j, conn[i][j], sml[i][j], cor[i][j]])
        rel_id += 1
rel = pd.DataFrame(rel,
                   columns=['rel_id', 'type', 'origin_id', 'destination_id', 'connection', 'similarity', 'correlation'])
rel.to_csv(dataname + '.rel', index=False)

dyna_id = 0
dyna_file = open(dataname + '.dyna', 'w')
dyna_file.write('dyna_id' + ',' + 'type' + ',' + 'time' + ',' + 'entity_id' + ',' + 'inflow' + ',' + 'outflow' + '\n')
date_set = {}
for i in range(data['x_train'].shape[2]):
    for date in range(18):
        for j in range(66):
            t = date * 66 + j
            time = str(data["xtime_train"][t][0]).split('.')[0] + 'Z'
            dyna_file.write(str(dyna_id) + ',' + 'state' + ',' + time + ',' + str(i) + ',' + str(
                data['x_train'][t][0][i][0]) + ',' + str(data['x_train'][t][0][i][1]) + '\n')
            dyna_id += 1
        t = date * 66 + 62
        for k in range(4):
            time = str(data["ytime_train"][t][k]).split('.')[0] + 'Z'
            dyna_file.write(str(dyna_id) + ',' + 'state' + ',' + time + ',' + str(i) + ',' + str(
                data['y_train'][t][k][i][0]) + ',' + str(data['y_train'][t][k][i][0]) + '\n')
            dyna_id += 1
        t = date * 66 + 65
        for k in range(1, 4):
            time = str(data["ytime_train"][t][k]).split('.')[0] + 'Z'
            dyna_file.write(str(dyna_id) + ',' + 'state' + ',' + time + ',' + str(i) + ',' + str(
                data['y_train'][t][k][i][0]) + ',' + str(data['y_train'][t][k][i][0]) + '\n')
            dyna_id += 1

    for date in range(2):
        for j in range(66):
            t = date * 66 + j
            time = str(data["xtime_val"][t][0]).split('.')[0] + 'Z'
            dyna_file.write(str(dyna_id) + ',' + 'state' + ',' + time + ',' + str(i) + ',' + str(
                data['x_val'][t][0][i][0]) + ',' + str(data['x_val'][t][0][i][1]) + '\n')
            dyna_id += 1
        t = date * 66 + 62
        for k in range(4):
            time = str(data["ytime_val"][t][k]).split('.')[0] + 'Z'
            dyna_file.write(str(dyna_id) + ',' + 'state' + ',' + time + ',' + str(i) + ',' + str(
                data['y_val'][t][k][i][0]) + ',' + str(data['y_val'][t][k][i][0]) + '\n')
            dyna_id += 1
        t = date * 66 + 65
        for k in range(1, 4):
            time = str(data["ytime_val"][t][k]).split('.')[0] + 'Z'
            dyna_file.write(str(dyna_id) + ',' + 'state' + ',' + time + ',' + str(i) + ',' + str(
                data['y_val'][t][k][i][0]) + ',' + str(data['y_val'][t][k][i][0]) + '\n')
            dyna_id += 1

    for date in range(5):
        for j in range(66):
            t = date * 66 + j
            time = str(data["xtime_test"][t][0]).split('.')[0] + 'Z'
            dyna_file.write(str(dyna_id) + ',' + 'state' + ',' + time + ',' + str(i) + ',' + str(
                data['x_test'][t][0][i][0]) + ',' + str(data['x_test'][t][0][i][1]) + '\n')
            dyna_id += 1
        t = date * 66 + 62
        for k in range(4):
            time = str(data["ytime_test"][t][k]).split('.')[0] + 'Z'
            dyna_file.write(str(dyna_id) + ',' + 'state' + ',' + time + ',' + str(i) + ',' + str(
                data['y_test'][t][k][i][0]) + ',' + str(data['y_test'][t][k][i][0]) + '\n')
            dyna_id += 1
        t = date * 66 + 65
        for k in range(1, 4):
            time = str(data["ytime_test"][t][k]).split('.')[0] + 'Z'
            dyna_file.write(str(dyna_id) + ',' + 'state' + ',' + time + ',' + str(i) + ',' + str(
                data['y_test'][t][k][i][0]) + ',' + str(data['y_test'][t][k][i][0]) + '\n')
            dyna_id += 1
dyna_file.close()

config = dict()
config['geo'] = dict()
config['geo']['including_types'] = ['Point']
config['geo']['Point'] = {}
config['rel'] = dict()
config['rel']['including_types'] = ['geo']
config['rel']['geo'] = {'connection': 'num', 'similarity': 'num', 'correlation': 'num'}
config['dyna'] = dict()
config['dyna']['including_types'] = ['state']
config['dyna']['state'] = {'entity_id': 'geo_id', 'inflow': 'num', 'outflow': 'num'}
config['info'] = dict()
config['info']['data_col'] = ['inflow', 'outflow']
config['info']['weight_col'] = ['connection']
config['info']['data_files'] = ['HZMETRO']
config['info']['geo_file'] = 'HZMETRO'
config['info']['rel_file'] = 'HZMETRO'
config['info']['output_dim'] = 2
config['info']['time_intervals'] = 900
config['info']['init_weight_inf_or_zero'] = 'inf'
config['info']['set_weight_link_or_dist'] = 'dist'
config['info']['calculate_weight_adj'] = False
config['info']['weight_adj_epsilon'] = 0.1
json.dump(config, open(outputdir + '/config.json', 'w', encoding='utf-8'), ensure_ascii=False)
