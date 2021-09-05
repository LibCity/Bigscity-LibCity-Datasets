# link: https://snap.stanford.edu/data/loc-brightkite.html
import pandas as pd
import os
output_folder = './output'
if not os.path.exists(output_folder):
    os.makedirs(output_folder)
traj = pd.read_csv('./input/Brightkite_totalCheckins.txt', sep='\t', names=['user', 'check_in_time', 'latitude', 'longitude', 'location_id'])

# 因为他没有单独的 usr 和 loc profile 因此需要先从 traj 中抽出 usr 和 poi 信息
# 需要把 location_id 重新编码
poi_info = traj.filter(items=['latitude', 'longitude', 'location_id'])
# 有些 poi 的经纬度有2个，感觉是定位定飘了，相差在百米范围内，因此取 mean
poi_info = poi_info.groupby('location_id').mean()
poi_info.reset_index(inplace=True)
poi_info['type'] = 'Point'

# 计算 coordinates
coordinates = []
for index, row in poi_info.iterrows():
    coordinates.append('[{},{}]'.format(row['longitude'], row['latitude']))

poi_info['coordinates'] = coordinates
poi_info = poi_info.drop(['latitude', 'longitude'], axis=1)
poi_info['geo_id'] = poi_info.index
traj = pd.merge(traj, poi_info, on='location_id')
traj = traj.drop(['location_id', 'type', 'coordinates'], axis=1)
poi_info = poi_info.drop(['location_id'], axis=1)
poi_info = poi_info.reindex(columns=['geo_id', 'type', 'coordinates'])
poi_info.to_csv(output_folder + '/brightkite.geo', index=False)

# 处理 usr
usr_info = pd.unique(traj['user'])
usr_info = pd.DataFrame(usr_info, columns=['usr_id'])
# 因为没有 properties 信息所以直接存吧
usr_info.to_csv(output_folder + '/brightkite.usr', index=False)

# 但是有 rel
rel = pd.read_csv('./Brightkite_edges.txt', sep='\t', names=['origin_id', 'destination_id'])
# rel 表中包含了一些没有出现在 check_in 里面的 usr，删去之
b = usr_info['usr_id'].tolist()
rel = rel[(rel['origin_id'].isin(b)) & (rel['destination_id'].isin(b))] 
rel['rel_id'] = rel.index
rel['type'] = 'usr'
rel = rel.reindex(columns=['rel_id', 'type', 'origin_id', 'destination_id'])
rel.to_csv(output_folder + '/brightkite.rel', index=False)

# 处理 traj
dyna = traj.drop(['latitude', 'longitude'], axis=1)
dyna = dyna.rename(columns={'user': 'entity_id', 'check_in_time': 'time', 'geo_id': 'location'})
dyna['type'] = 'trajectory'
# 按照时间的先后顺序排序该 dyna
dyna = dyna.sort_values(by='time')
dyna = dyna.reset_index(drop=True)
dyna['dyna_id'] = dyna.index
dyna = dyna.reindex(columns=['dyna_id', 'type', 'time', 'entity_id', 'location'])
dyna.to_csv(output_folder + '/brightkite.dyna', index=False)
