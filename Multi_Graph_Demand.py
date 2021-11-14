import pandas as pd
import numpy as np
import os
import datetime
import util
import json

outputdir = 'output/Multi_Graph_Demand'
util.ensure_dir(outputdir)

dataurl = 'input/Multi_Graph_Demand/'
dataname = outputdir + '/Multi_Graph_Demand'
util.ensure_dir(dataurl)

NODE_NUM = 30
poi_30_path = os.path.join(dataurl, "poi_30.csv")
point_location_path = os.path.join(dataurl,"PointsID.csv")

poi_array = pd.read_csv(poi_30_path).values
point_location_array = pd.read_csv(point_location_path,header=None).values

def find_location(point_id):
    for i in range(len(point_location_array)):
        if point_id == point_location_array[i][0]:
            x,y = point_location_array[i][1],point_location_array[i][2]
            item = []
            item.append("%.6f"%x)
            item.append("%.6f"%y)
            return "["+",".join(item)+"]"
    raise Exception("not find")

geo_data = []
for raw in range(NODE_NUM):
    item = []
    item.append(str(raw))
    item.append("Point")
    point_id = poi_array[raw][0]
    item.append(find_location(point_id))
    for i in range(1,len(poi_array[raw])):
        item.append(poi_array[raw][i])
    geo_data.append(item)

poi_types = list(pd.read_csv(poi_30_path).columns)
geo = pd.DataFrame(geo_data,columns=['geo_id','type','coordinates']+poi_types[1:])
geo.to_csv(dataname+'.geo',index=False)


def weight_matrix(file_path, sigma2=0.1, epsilon=0.5, scaling=True):
    try:
        W = pd.read_csv(file_path, header=None).values
    except FileNotFoundError:
        print('input file was not found')

    # check whether W is a 0/1 matrix.
    if set(np.unique(W)) == {0, 1}:
        print('The input graph is a 0/1 matrix; set "scaling" to False.')
        scaling = False

    if scaling:
        n = W.shape[0]
        W = W / 10000.
        W2, W_mask = W * W, np.ones([n, n]) - np.identity(n)
        # refer to Eq.10
        return np.exp(-W2 / sigma2) * (np.exp(-W2 / sigma2) >= epsilon) * W_mask
    else:
        return W

simi_30_path = os.path.join(dataurl, "weight_simi.csv")
adj_30_path = os.path.join(dataurl, "weight_adj.csv")
distance_path = os.path.join(dataurl, "PointsDistance.csv")

dis_weight = weight_matrix(distance_path)
dis_array = pd.DataFrame(dis_weight)
adj_array = pd.read_csv(adj_30_path,header=None).values
simi_array = pd.read_csv(simi_30_path,header=None).values

rel_data = []
rel_id = 0
for i in range(30):
    for j in range(30):
        item = []
        item.append(rel_id)
        item.append("geo")
        item.append(i),item.append(j)
        item.append(dis_array[i][j])
        item.append(adj_array[i][j])
        item.append(simi_array[i][j])
        rel_data.append(item)
        rel_id += 1


rel = pd.DataFrame(rel_data,columns=['rel_id','type','origin_id','destination_id','distance','connection','similarity'])
rel.to_csv(dataname+'.rel',index=False)


all_data_path = os.path.join(dataurl, "data.csv")
raw_grid_df = pd.read_csv(all_data_path,header=None)
raw_grid_array = np.array(raw_grid_df)
grid_array = []
dyna_id = 0
now_time = datetime.datetime(year=2017,month=1,day=1)

for i in range(len(raw_grid_array)):
    for j in range(30):
        item = []
        item.append(dyna_id)
        item.append("state")
        now_time_str = now_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        item.append(now_time_str)
        item.append(j)
        item.append(raw_grid_array[i][j])
        grid_array.append(item)
        dyna_id += 1
    now_time = now_time + datetime.timedelta(minutes=5)
grid_array = sorted(grid_array,key=(lambda x:x[3]))
for i in range(len(grid_array)):
    grid_array[i][0] = i
grid = pd.DataFrame(grid_array,columns=['geo_id','type','coordinates','entity_id','demand'])
grid.to_csv(dataname+'.dyna',index=False)



config = dict()
config['geo'] = dict()
config['geo']['including_types'] = ['Point']
config['geo']['Point'] = {}
config['rel'] = dict()
config['rel']['including_types'] = ['geo']
config['rel']['geo'] = {'distance':'num','connection': 'num','similarity': 'num'}
config['dyna'] = dict()
config['dyna']['including_types'] = ['state']
config['dyna']['state'] = {'entity_id': 'geo_id', 'demand': 'num'}
config['info'] = dict()
config['info']['data_col'] = ['demand']
config['info']['data_files'] = ['Multi_Graph_Demand']
config['info']['geo_file'] = 'Multi_Graph_Demand'
config['info']['rel_file'] = 'Multi_Graph_Demand'

config['info']['output_dim'] = 2 #???
config['info']['time_intervals'] = 40*24*12
config['info']['init_weight_inf_or_zero'] = 'zero'
config['info']['set_weight_link_or_dist'] = 'dist'
config['info']['calculate_weight_adj'] = False
config['info']['weight_adj_epsilon'] = 0.1 #??
json.dump(config, open(outputdir + '/config.json', 'w', encoding='utf-8'), ensure_ascii=False)