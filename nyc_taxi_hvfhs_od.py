import numpy as np
import pandas as pd
import geopandas as gpd
from shapely import wkt
import geojson
import json
import os
import util


outputdir = 'output/NYC_TAXI_Hvfhs_od'
util.ensure_dir(outputdir)

dataurl = 'input/NYC-Taxi/NYC-Taxi 2020-06/fhvhv_tripdata_2020-06.csv'
lookupurl = 'input/NYC-Taxi/taxi_zones/taxi_zones.shp'
dataname = outputdir+'/NYC_TAXI_Hvfhs'

shp = gpd.read_file(lookupurl)
shp = pd.DataFrame(shp)
geo = []
for i in range(shp.shape[0]):
    id = shp['LocationID'][i]
    Shape_Leng = shp['Shape_Leng'][i]
    Shape_Area = shp['Shape_Area'][i]
    zone = shp['zone'][i]
    borough = shp['borough'][i]
    geometry = str(shp['geometry'][i])
    feature_json = geojson.Feature(geometry=wkt.loads(geometry), properties={})
    geo.append([id, feature_json['geometry']['type'],
                '['+str(feature_json['geometry']['coordinates'])+']',
                Shape_Leng, Shape_Area, zone, borough])
geo = pd.DataFrame(geo, columns=['geo_id', 'type', 'coordinates', 'Shape_Leng', 'Shape_Area', 'zone', 'borough'])
geo = geo.sort_values(by='geo_id')
geo.to_csv(dataname+'.geo', index=False)


dataset = pd.read_csv(dataurl, low_memory=False)
# 有意义的ID有范围[1,263]
dataset = dataset[(dataset['PULocationID']<=263) & (dataset['PULocationID']>0)
                  & (dataset['DOLocationID']<=263) & (dataset['DOLocationID']>0)]
# 去空值
dataset = dataset.drop('SR_Flag', axis=1)
# dataset = dataset[~dataset.isnull().T.any()]
# 重排索引!!
dataset = dataset.reset_index(drop=True)


usr = []
for i in range(dataset.shape[0]):
    usr.append([i])
usr = pd.DataFrame(usr, columns=['usr_id'])
usr.to_csv(dataname+'.usr', index=False)


rel = []
reldict = dict()
dyna_od = []
reldata = dataset[['PULocationID', 'DOLocationID']]
for i in range(reldata.shape[0]):
    sid = reldata['PULocationID'][i]
    eid = reldata['DOLocationID'][i]
    if (sid, eid) not in reldict:
        reldict[(sid, eid)] = len(reldict)
        rel.append([len(reldict) - 1, 'geo', sid, eid])
    dyna_od.append(reldict[(sid, eid)])
    if i % 10000 == 0:
        print(str(i)+'/'+str(reldata.shape[0]))
rel = pd.DataFrame(rel, columns=['rel_id', 'type', 'origin_id', 'destination_id'])
rel.to_csv(dataname+'.rel', index=False)


# 把两列id翻译成od_id/rel_id
# dyna_od = []
# for i in range(dataset.shape[0]):
#     sid = dataset['PULocationID'][i]
#     eid = dataset['DOLocationID'][i]
#     dyna_od.append(reldict[(sid, eid)])

# 把两列time翻译成固定格式
# dyna_time = []
# for i in range(dataset.shape[0]):
#     stime = util.add_TZ(dataset['pickup_datetime'][i])
#     etime = util.add_TZ(dataset['dropoff_datetime'][i])
#     dyna_time.append(str(stime)+' '+str(etime))
#     if i % 10000 == 0:
#         print(str(i)+'/'+str(dataset.shape[0]))

# 把两列time翻译成固定格式
dyna_time = []
for i in range(dataset.shape[0]):
    stime = dataset['pickup_datetime'][i].replace(' ', 'T')+'Z'
    etime = dataset['dropoff_datetime'][i].replace(' ', 'T')+'Z'
    dyna_time.append(str(stime)+' '+str(etime))
    if i % 10000 == 0:
        print(str(i)+'/'+str(dataset.shape[0]))

dyna = dataset
dyna = dyna.drop('PULocationID', axis=1)
dyna = dyna.drop('DOLocationID', axis=1)
dyna = dyna.drop('pickup_datetime', axis=1)
dyna = dyna.drop('dropoff_datetime', axis=1)

dyna.insert(0, 'dyna_id', range(dyna.shape[0]))
dyna.insert(1, 'type', dyna.shape[0] * ['od'])
dyna.insert(2, 'time', dyna_time)
dyna.insert(3, 'entity_id', range(dyna.shape[0]))
dyna.insert(4, 'od_id', dyna_od)
dyna.to_csv(dataname+'.dyna', index=False)


config = dict()
config['geo'] = dict()
config['geo']['including_types'] = ['Polygon', 'MultiPolygon']
config['geo']['Polygon'] = {'Shape_Leng': 'num', 'Shape_Area': 'num', 'zone': 'other', 'borough': 'other'}
config['geo']['MultiPolygon'] = {'Shape_Leng': 'num', 'Shape_Area': 'num', 'zone': 'other', 'borough': 'other'}
config['usr'] = dict()
config['usr']['properties'] = {}
config['rel'] = dict()
config['rel']['including_types'] = ['geo']
config['rel']['geo'] = {}
config['dyna'] = dict()
config['dyna']['including_types'] = ['od']
config['dyna']['od'] = {'entity_id': 'usr_id', 'od_id': 'rel_id',
                        'hvfhs_license_num': 'enum', 'dispatching_base_num': 'enum'}
json.dump(config, open(outputdir+'/config.json', 'w', encoding='utf-8'), ensure_ascii=False)
