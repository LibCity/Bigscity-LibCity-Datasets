import json
import util
import pandas as pd
import numpy as np

# 输出文件所在的目录
output_dir = 'output/Loop Seattle'
util.ensure_dir(output_dir)

#
data_url = "input/Loop Seattle/Seattle_Loop_Dataset/"
# 输出文件的前缀
output_name = output_dir + "/Loop_Seattle"

dataset = pd.read_csv(data_url + "nodes_loop_mp_list.csv")
id_set = set()
milepost_name_dict = dict()
geo = []
properties = dict()
for i in range(dataset.shape[0]):
    index = i
    milepost_name = str(dataset['milepost'][i])
    if index not in id_set:
        milepost_name_dict[milepost_name] = index
        id_set.add(index)
        geo.append([index, 'Point', milepost_name])
geo = pd.DataFrame(geo, columns=['geo_id', 'type', 'geo_name'])
geo.to_csv(output_name + ".geo", index=False)

adj_mx = np.load(data_url + "Loop_Seattle_2015_A.npy").astype(np.int16)
reachability_free_flow_5min = np.load(data_url + "Loop_Seattle_2015_reachability_free_flow_5min.npy").astype(np.int16)
reachability_free_flow_10min = np.load(data_url + "Loop_Seattle_2015_reachability_free_flow_10min.npy").astype(np.int16)
reachability_free_flow_15min = np.load(data_url + "Loop_Seattle_2015_reachability_free_flow_15min.npy").astype(np.int16)
reachability_free_flow_20min = np.load(data_url + "Loop_Seattle_2015_reachability_free_flow_20min.npy").astype(np.int16)
reachability_free_flow_25min = np.load(data_url + "Loop_Seattle_2015_reachability_free_flow_25min.npy").astype(np.int16)
rel = []
for i in range(adj_mx.shape[0]):
    for j in range(adj_mx.shape[1]):
        sid = i
        eid = j
        rel.append([len(rel), 'geo', sid, eid, adj_mx[i][j],
                    reachability_free_flow_5min[i][j],
                    reachability_free_flow_10min[i][j],
                    reachability_free_flow_15min[i][j],
                    reachability_free_flow_20min[i][j],
                    reachability_free_flow_25min[i][j], ])
rel = pd.DataFrame(rel, columns=['rel_id', 'type', 'origin_id', 'destination_id',
                                 'adj',
                                 'FFR_5min',
                                 'FFR_10min',
                                 'FFR_15min',
                                 'FFR_20min',
                                 'FFR_25min'])
rel.to_csv(output_name + ".rel", index=False)

speed_mx = pd.read_pickle(data_url + 'speed_matrix_2015')
milepost_list = list(speed_mx.columns)
time_slot = np.array(speed_mx.index.values)
dataset = pd.DataFrame(speed_mx.values, columns=milepost_list)
dyna_id = 0
dyna_file = open(output_name + ".dyna", 'w')
dyna_file.write('dyna_id' + ',' + 'type' + ',' + 'time' + ',' + 'entity_id' + ',' + 'traffic_speed' + '\n')
for j in range(len(milepost_list)):
    entity_name = str(milepost_list[j])
    entity_id = milepost_name_dict[entity_name]
    for i in range(time_slot.shape[0]):
        time = str(time_slot[i].split()[0]) + "T" + str(time_slot[i].split()[1]) + "Z"
        dyna_file.write(str(dyna_id) + ',' + 'state' + ',' + str(time) + ',' + str(entity_id) + ',' + str(
            dataset[milepost_list[j]][i]) + '\n')
        dyna_id = dyna_id + 1
        if dyna_id % 10000 == 0:
            print(str(dyna_id) + '/' + str(time_slot.shape[0] * len(milepost_list)))
dyna_file.close()

config = dict()
config['geo'] = dict()
config['geo']['including_types'] = ['Point']
config['geo']['Point'] = {'geo_name': 'other'}
config['rel'] = dict()
config['rel']['including_types'] = ['geo']
config['rel']['geo'] = {'adj': 'num',
                        'FFR_5min': 'num',
                        'FFR_10min': 'num',
                        'FFR_15min': 'num',
                        'FFR_20min': 'num',
                        'FFR_25min': 'num'}
config['dyna'] = dict()
config['dyna']['including_types'] = ['state']
config['dyna']['state'] = {'entity_id': 'geo_id', 'traffic_speed': 'num'}
config['info'] = dict()
config['info']['data_col'] = ['traffic_speed']
config['info']['weight_col'] = 'adj'
config['info']['data_files'] = ['Loop Seattle']
config['info']['geo_file'] = 'Loop Seattle'
config['info']['rel_file'] = 'Loop Seattle'
config['info']['output_dim'] = 1
config['info']['init_weight_inf_or_zero'] = 'inf'
config['info']['set_weight_link_or_dist'] = 'dist'
config['info']['calculate_weight_adj'] = False
config['info']['weight_adj_epsilon'] = 0.1
json.dump(config, open(output_dir + '/config.json', 'w', encoding='utf-8'), ensure_ascii=False)
