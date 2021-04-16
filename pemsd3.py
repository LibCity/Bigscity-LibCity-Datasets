import numpy as np
import pandas as pd
import json
import util


outputdir = 'output/PEMSD3'
util.ensure_dir(outputdir)

dataurl = 'input/PEMS03/'
dataname = outputdir+'/PEMSD3'

dataset = pd.read_csv(dataurl+'PEMS03.txt', header=None)
idlist = list(dataset[0])
geo = []
for i in idlist:
    geo.append([i, 'Point', '[]'])
geo = pd.DataFrame(geo, columns=['geo_id', 'type', 'coordinates'])
geo.to_csv(dataname+'.geo', index=False)


rel = []
reldict = dict()
dataset = pd.read_csv(dataurl+'PEMS03.csv')
for i in range(dataset.shape[0]):
    sid = dataset['from'][i]
    eid = dataset['to'][i]
    if sid not in idlist or eid not in idlist:
        continue
    distance = dataset['distance'][i]
    if (sid, eid) not in reldict:
        reldict[(sid, eid)] = len(reldict)
        rel.append([len(reldict) - 1, 'geo', sid, eid, distance])
rel = pd.DataFrame(rel, columns=['rel_id', 'type', 'origin_id', 'destination_id', 'distance'])
rel.to_csv(dataname+'.rel', index=False)


dataset = np.load(dataurl+'PEMS03.npz')['data']  # (26208, 358, 1)

start_time = util.datetime_timestamp('2018-09-01T00:00:00Z')
end_time = util.datetime_timestamp('2018-12-01T00:00:00Z')
timeslot = []
while start_time < end_time:
    timeslot.append(util.timestamp_datetime(start_time))
    start_time = start_time + 5*60

# dyna = []
dyna_id = 0
dyna_file = open(dataname+'.dyna', 'w')
dyna_file.write('dyna_id' + ',' + 'type' + ',' + 'time' + ',' + 'entity_id' + ',' + 'traffic_flow' + '\n')
for j in range(len(idlist)):
    entity_id = idlist[j]
    for i in range(len(timeslot)):
        time = timeslot[i]
        # dyna.append([dyna_id, 'state', time, entity_id, dataset[i][j][0]])
        dyna_file.write(str(dyna_id) + ',' + 'state' + ',' + str(time)
                        + ',' + str(entity_id) + ',' + str(dataset[i][j][0]) + '\n')
        dyna_id = dyna_id + 1
        if dyna_id % 10000 == 0:
            print(str(dyna_id) + '/' + str(dataset.shape[0]*dataset.shape[1]))
dyna_file.close()
# dyna = pd.DataFrame(dyna, columns=['dyna_id', 'type', 'time', 'entity_id', 'traffic_flow'])
# dyna.to_csv(dataname+'.dyna', index=False)


config = dict()
config['geo'] = dict()
config['geo']['including_types'] = ['Point']
config['geo']['Point'] = {}
config['rel'] = dict()
config['rel']['including_types'] = ['geo']
config['rel']['geo'] = {'distance': 'num'}
config['dyna'] = dict()
config['dyna']['including_types'] = ['state']
config['dyna']['state'] = {'entity_id': 'geo_id', 'traffic_flow': 'num'}
config['info'] = dict()
config['info']['data_col'] = ['traffic_flow']
config['info']['weight_col'] = 'distance'
config['info']['data_files'] = ['PEMSD3']
config['info']['geo_file'] = 'PEMSD3'
config['info']['rel_file'] = 'PEMSD3'
config['info']['output_dim'] = 1
config['info']['time_intervals'] = 300
config['info']['init_weight_inf_or_zero'] = 'zero'
config['info']['set_weight_link_or_dist'] = 'link'
config['info']['calculate_weight_adj'] = False
config['info']['weight_adj_epsilon'] = 0.1
json.dump(config, open(outputdir+'/config.json', 'w', encoding='utf-8'), ensure_ascii=False)
