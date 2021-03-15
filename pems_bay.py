import numpy as np
import pandas as pd
import json
import util


outputdir = 'output/PEMS_BAY'
util.ensure_dir(outputdir)

dataurl = 'input/PEMS-BAY/'
dataname = outputdir+'/PEMS_BAY'

dataset = pd.read_csv(dataurl+'sensor_graph/graph_sensor_locations_bay.csv', header=None)
dataset.columns = ['sensor_id', 'latitude', 'longitude']
idset = set()
geo = []
for i in range(dataset.shape[0]):
    id = dataset['sensor_id'][i]
    lat = dataset['latitude'][i]
    lon = dataset['longitude'][i]
    if id not in idset:
        idset.add(id)
        geo.append([id, 'Point', '['+str(lon)+', '+str(lat)+']'])
geo = pd.DataFrame(geo, columns=['geo_id', 'type', 'coordinates'])
geo.to_csv(dataname+'.geo', index=False)


rel = []
reldict = dict()
dataset = pd.read_csv(dataurl+'sensor_graph/distances_bay_2017.csv', header=None)
dataset.columns = ['from', 'to', 'cost']
for i in range(dataset.shape[0]):
    sid = dataset['from'][i]
    eid = dataset['to'][i]
    if sid not in idset or eid not in idset:
        continue
    cost = dataset['cost'][i]
    if (sid, eid) not in reldict:
        reldict[(sid, eid)] = len(reldict)
        rel.append([len(reldict) - 1, 'geo', sid, eid, cost])
rel = pd.DataFrame(rel, columns=['rel_id', 'type', 'origin_id', 'destination_id', 'cost'])
rel.to_csv(dataname+'.rel', index=False)


# dataset = h5py.File(dataurl+'pems-bay.h5', 'r')
# df = dataset['df']
# senior = np.array(df['axis0'])
# seniorlist = []
# for i in range(senior.shape[0]):
#     seniorlist.append(bytes.decode(senior[i]))
# timeslot = np.array(df['axis1'])
# dataset = pd.DataFrame(df['block0_values'], columns=seniorlist)

df = pd.read_hdf(dataurl+'pems-bay.h5')
seniorlist = list(df.columns)
timeslot = np.array(df.index.values)
dataset = pd.DataFrame(df.values, columns=seniorlist)

# dyna = []
dyna_id = 0
dyna_file = open(dataname+'.dyna', 'w')
dyna_file.write('dyna_id' + ',' + 'type' + ',' + 'time' + ',' + 'entity_id' + ',' + 'traffic_speed' + '\n')
for j in range(len(seniorlist)):
    entity_id = int(seniorlist[j])
    for i in range(timeslot.shape[0]):
        time = str(timeslot[i])[:-10]+'Z'
        # dyna.append([dyna_id, 'state', time, entity_id, dataset[seniorlist[j]][i]])
        dyna_file.write(str(dyna_id) + ',' + 'state' + ',' + str(time)
                        + ',' + str(entity_id) + ',' + str(dataset[seniorlist[j]][i]) + '\n')
        dyna_id = dyna_id + 1
        if dyna_id % 10000 == 0:
            print(str(dyna_id) + '/' + str(timeslot.shape[0]*len(seniorlist)))
dyna_file.close()
# dyna = pd.DataFrame(dyna, columns=['dyna_id', 'type', 'time', 'entity_id', 'traffic_speed'])
# dyna.to_csv(dataname+'.dyna', index=False)


config = dict()
config['geo'] = dict()
config['geo']['including_types'] = ['Point']
config['geo']['Point'] = {}
config['rel'] = dict()
config['rel']['including_types'] = ['geo']
config['rel']['geo'] = {'cost': 'num'}
config['dyna'] = dict()
config['dyna']['including_types'] = ['state']
config['dyna']['state'] = {'entity_id': 'geo_id', 'traffic_speed': 'num'}
config['info'] = dict()
config['info']['data_col'] = 'traffic_speed'
config['info']['weight_col'] = 'cost'
config['info']['data_files'] = ['PEMS_BAY']
config['info']['geo_file'] = 'PEMS_BAY'
config['info']['rel_file'] = 'PEMS_BAY'
json.dump(config, open(outputdir+'/config.json', 'w', encoding='utf-8'), ensure_ascii=False)
