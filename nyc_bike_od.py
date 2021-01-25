import numpy as np
import pandas as pd
import json
import os
import util


outputdir = 'output/NYC_BIKE_od'
util.ensure_dir(outputdir)

dataurl = 'input/NYC-Bike/JC-202009-citibike-tripdata.csv/JC-202009-citibike-tripdata.csv'
dataname = outputdir+'/NYC_BIKE'
dataset = pd.read_csv(dataurl)

idset = set()
geo = []
start = dataset[['start station id', 'start station name', 'start station latitude', 'start station longitude']]
for i in range(start.shape[0]):
    id = start['start station id'][i]
    name = start['start station name'][i]
    lat = start['start station latitude'][i]
    lon = start['start station longitude'][i]
    if id not in idset:
        idset.add(id)
        geo.append([id, 'Point', '['+str(lon)+', '+str(lat)+']', str(name)])

end = dataset[['end station id', 'end station name', 'end station latitude', 'end station longitude']]
for i in range(end.shape[0]):
    id = end['end station id'][i]
    name = end['end station name'][i]
    lat = end['end station latitude'][i]
    lon = end['end station longitude'][i]
    if id not in idset:
        idset.add(id)
        geo.append([id, 'Point', '['+str(lon)+', '+str(lat)+']', str(name)])

geo = pd.DataFrame(geo, columns=['geo_id', 'type', 'coordinates', 'poi_name'])
geo = geo.sort_values(by='geo_id')
geo.to_csv(dataname+'.geo', index=False)


usr = []
userdata = dataset[['usertype', 'birth year', 'gender']]
for i in range(userdata.shape[0]):
    usertype = userdata['usertype'][i]
    birth = userdata['birth year'][i]
    gender = userdata['gender'][i]
    usr.append([i, usertype, birth, gender])
usr = pd.DataFrame(usr, columns=['usr_id', 'usertype', 'birth_year', 'gender'])
usr.to_csv(dataname+'.usr', index=False)


rel = []
reldict = dict()
reldata = dataset[['start station id', 'end station id']]
for i in range(reldata.shape[0]):
    sid = reldata['start station id'][i]
    eid = reldata['end station id'][i]
    assert sid in idset
    assert eid in idset
    if (sid, eid) not in reldict:
        reldict[(sid, eid)] = len(reldict)
        rel.append([len(reldict) - 1, 'geo', sid, eid])
rel = pd.DataFrame(rel, columns=['rel_id', 'type', 'origin_id', 'destination_id'])
rel.to_csv(dataname+'.rel', index=False)


dyna = []
for i in range(dataset.shape[0]):
    tripduration = dataset['tripduration'][i]
    stime = util.add_TZ(dataset['starttime'][i][:-5])
    etime = util.add_TZ(dataset['stoptime'][i][:-5])
    sid = dataset['start station id'][i]
    eid = dataset['end station id'][i]
    bikeid = dataset['bikeid'][i]
    dyna.append([i, 'od', str(stime)+' '+str(etime), i, reldict[(sid, eid)], bikeid, tripduration])
dyna = pd.DataFrame(dyna, columns=['dyna_id', 'type', 'time', 'entity_id', 'od_id', 'bikeid', 'trip_duration'])
dyna.to_csv(dataname+'.dyna', index=False)


config = dict()
config['geo'] = dict()
config['geo']['including_types'] = ['Point']
config['geo']['Point'] = {'poi_name': 'other'}
config['usr'] = dict()
config['usr']['properties'] = {'usertype': 'other', 'birth_year': 'num', 'gender': 'num'}
config['rel'] = dict()
config['rel']['including_types'] = ['geo']
config['rel']['geo'] = {}
config['dyna'] = dict()
config['dyna']['including_types'] = ['od']
config['dyna']['od'] = {'entity_id': 'usr_id', 'od_id': 'rel_id', 'bikeid': 'other', 'trip_duration': 'num'}
json.dump(config, open(outputdir+'/config.json', 'w', encoding='utf-8'), ensure_ascii=False)
