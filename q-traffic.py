import pandas as pd
from tqdm import tqdm
import json
import util

outputdir = 'output/Q-TRAFFIC'
util.ensure_dir(outputdir)

dataurl = 'input/Q-TRAFFIC/'
dataname = outputdir + '/Q-TRAFFIC'


def id_to_time(id):
    year = 2017
    month = 4
    day = int(id / (24 * 4))
    ori_day = day
    if day >= 30:
        month = 5
        day -= 30
    hour = int((id - 24 * 4 * ori_day) / 4)
    min = int(id - 24 * 4 * ori_day - 4 * hour) * 15
    sec = 0
    day += 1
    return str(year).zfill(4) + "-" + str(month).zfill(2) + "-" + str(day).zfill(2) + \
        "T" + str(hour).zfill(2) + ":" + str(min).zfill(2) + ":" + str(sec).zfill(2) + "Z"


dataset = pd.read_csv(dataurl + 'road_network_sub-dataset.v2/road_network_sub-dataset.v2', sep='\t', header=None)
ori_columns_num = len(dataset.columns)
for i in range(8, ori_columns_num):
    dataset = dataset.drop([i], axis=1)
firstline = [1562548955, 30, 3, 1520445066, 1549742690, 0.038, 6, 1]
for i in range(8):
    dataset.at[0, i] = firstline[i]

linkset = pd.read_csv(dataurl + 'road_network_sub-dataset.v2/link_gps.v2', sep='\t', header=None)
idlist = linkset[0].values.tolist()
idset = set()
geo = []
node_inlink_dict = {}
node_outlink_dict = {}

datanum = dataset.shape[0]
for i in range(datanum):
    id = dataset[0][i]
    lat = linkset[2][i]
    lon = linkset[1][i]
    width = dataset[1][i]
    direction = str(dataset[2][i])
    snodeid = dataset[3][i]
    enodeid = dataset[4][i]
    length = dataset[5][i]
    speedclass = str(dataset[6][i])
    lanenum = dataset[7][i]
    if id not in idset:
        idset.add(id)
        if direction == "0" or direction == "1":
            if snodeid not in node_inlink_dict.keys():
                node_inlink_dict[snodeid] = []
            if enodeid not in node_inlink_dict.keys():
                node_inlink_dict[enodeid] = []
            node_inlink_dict[snodeid].append(id)
            node_inlink_dict[enodeid].append(id)

            if snodeid not in node_outlink_dict.keys():
                node_outlink_dict[snodeid] = []
            if enodeid not in node_outlink_dict.keys():
                node_outlink_dict[enodeid] = []
            node_outlink_dict[snodeid].append(id)
            node_outlink_dict[enodeid].append(id)
        elif direction == "2":
            if enodeid not in node_inlink_dict.keys():
                node_inlink_dict[enodeid] = []
            node_inlink_dict[enodeid].append(id)

            if snodeid not in node_outlink_dict.keys():
                node_outlink_dict[snodeid] = []
            node_outlink_dict[snodeid].append(id)
        elif direction == "3":
            if snodeid not in node_inlink_dict.keys():
                node_inlink_dict[snodeid] = []
            node_inlink_dict[snodeid].append(id)

            if enodeid not in node_outlink_dict.keys():
                node_outlink_dict[enodeid] = []
            node_outlink_dict[enodeid].append(id)

        geo.append([id, 'LineString', '[['+str(lon)+', '+str(lat)+']]',
                    width, direction, snodeid, enodeid,
                    length, speedclass, lanenum])
geo = pd.DataFrame(geo, columns=['geo_id', 'type', 'coordinates',
                                 'width', 'direction', 'snodeid', 'enodeid',
                                 'length', 'speedclass', 'lanenum'])
geo.to_csv(dataname+'.geo', index=False)


rel = []
reldict = dict()

for i in range(datanum):
    sid = dataset[0][i]
    direction = str(dataset[2][i])
    snodeid = dataset[3][i]
    enodeid = dataset[4][i]

    relids = set()
    if direction == "0" or direction == "1":
        if enodeid in node_inlink_dict and enodeid in node_outlink_dict:
            relids = relids.union(set(node_outlink_dict[enodeid]))
            for eid in relids:
                if eid == sid:
                    continue
                link_weight = 1
                if (sid, eid) not in reldict:
                    reldict[(sid, eid)] = len(reldict)
                    rel.append([len(reldict) - 1, 'geo', sid, eid, link_weight])
        if snodeid in node_inlink_dict and snodeid in node_outlink_dict:
            relids = relids.union(set(node_outlink_dict[snodeid]))
            for eid in relids:
                if eid == sid:
                    continue
                link_weight = 1
                if (sid, eid) not in reldict:
                    reldict[(sid, eid)] = len(reldict)
                    rel.append([len(reldict) - 1, 'geo', sid, eid, link_weight])
    elif direction == "2":
        if enodeid in node_inlink_dict and enodeid in node_outlink_dict:
            relids = relids.union(set(node_outlink_dict[enodeid]))
            for eid in relids:
                if eid == sid:
                    continue
                link_weight = 1
                if (sid, eid) not in reldict:
                    reldict[(sid, eid)] = len(reldict)
                    rel.append([len(reldict) - 1, 'geo', sid, eid, link_weight])
    elif direction == "3":
        if snodeid in node_inlink_dict and snodeid in node_outlink_dict:
            relids = relids.union(set(node_outlink_dict[snodeid]))
            for eid in relids:
                if eid == sid:
                    continue
                link_weight = 1
                if (sid, eid) not in reldict:
                    reldict[(sid, eid)] = len(reldict)
                    rel.append([len(reldict) - 1, 'geo', sid, eid, link_weight])

rel = pd.DataFrame(rel, columns=['rel_id', 'type', 'origin_id', 'destination_id', 'link_weight'])
rel.to_csv(dataname+'.rel', index=False)


dataset = pd.read_csv(dataurl + 'traffic_speed_sub-dataset.v2/traffic_speed_sub-dataset.v2', sep=', ', header=None)

dyna_id = 0
dyna_file = open(dataname+'.dyna', 'w')
dyna_file.write('dyna_id' + ',' + 'type' + ',' + 'time' + ',' + 'entity_id' + ',' + 'traffic_speed' + '\n')
datanum = dataset.shape[0]
for i in tqdm(range(datanum)):
    entity_id = dataset[0][i]
    time_id = dataset[1][i]
    traffic_speed = dataset[2][i]
    time = id_to_time(time_id)
    dyna_file.write(str(dyna_id) + ',' + 'state' + ',' + str(time)
                    + ',' + str(entity_id) + ',' + str(traffic_speed) + '\n')
    dyna_id += 1
dyna_file.close()


config = dict()
config['geo'] = dict()
config['geo']['including_types'] = ['LineString']
config['geo']['LineString'] = {
    'width': 'num',
    'direction': 'enum',
    'snodeid': 'num',
    'enodeid': 'num',
    'length': 'num',
    'speedclass': 'enum',
    'lanenum': 'num'
}
config['rel'] = dict()
config['rel']['including_types'] = ['geo']
config['rel']['geo'] = {'link_weight': 'num'}
config['dyna'] = dict()
config['dyna']['including_types'] = ['state']
config['dyna']['state'] = {'entity_id': 'geo_id', 'traffic_speed': 'num'}
config['info'] = dict()
config['info']['data_col'] = 'traffic_speed'
config['info']['weight_col'] = 'link_weight'
config['info']['data_files'] = ['Q-TRAFFIC']
config['info']['geo_file'] = 'Q-TRAFFIC'
config['info']['rel_file'] = 'Q-TRAFFIC'
config['info']['output_dim'] = 1
config['info']['time_intervals'] = 900
config['info']['init_weight_inf_or_zero'] = 'zero'
config['info']['set_weight_link_or_dist'] = 'link'
config['info']['calculate_weight_adj'] = False
config['info']['weight_adj_epsilon'] = 0.1
json.dump(config, open(outputdir+'/config.json', 'w', encoding='utf-8'), ensure_ascii=False)
