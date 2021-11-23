# link: https://sites.google.com/site/yangdingqi/home/foursquare-dataset
# we use the second dataset
import pandas as pd
from tqdm import tqdm
from datetime import datetime, timedelta
import os
raw_data = pd.read_csv('./input/dataset_TSMC2014_NYC.txt', sep='\t', header=None,
                       names=['User ID', 'Venue ID', 'Venue category ID', 'Venue category name', 'Latitude',
                              'Longitude', 'Timezone offset in minutes', 'UTC time'], encoding='ISO-8859-1')
output_folder = './output'
if not os.path.exists(output_folder):
    os.makedirs(output_folder)
# 先处理 geo
poi = raw_data.filter(items=['Venue ID', 'Latitude', 'Longitude'])
poi = poi.groupby('Venue ID').mean()
poi.reset_index(inplace=True)
poi['geo_id'] = poi.index

# 计算 coordinates
coordinates = []
for index, row in poi.iterrows():
    coordinates.append('[{},{}]'.format(row['Longitude'], row['Latitude']))

poi['coordinates'] = coordinates
poi['type'] = 'Point'
poi = poi.drop(['Latitude', 'Longitude'], axis=1)
category_info = raw_data.filter(items=['Venue ID','Venue category ID', 'Venue category name'])
category_info = category_info.rename(columns={'Venue category name': 'venue_category_name',
                                              'Venue category ID': 'venue_category_id'})
category_info = category_info.drop_duplicates(['Venue ID'])
poi = pd.merge(poi, category_info, on='Venue ID')
loc_hash2ID = poi.filter(items=['Venue ID', 'geo_id'])
poi = poi.reindex(columns=['geo_id', 'type', 'coordinates', 'venue_category_id', 'venue_category_name'])
poi.to_csv(output_folder + '/foursquare_nyc.geo', index=False)

# 处理 usr
user = pd.unique(raw_data['User ID'])
user = pd.DataFrame(user, columns=['User ID'])
user['usr_id'] = user.index

# 处理 dyna
dyna = raw_data.filter(items=['User ID', 'Venue ID', 'Timezone offset in minutes', 'UTC time'])
dyna = pd.merge(dyna, loc_hash2ID, on='Venue ID')
dyna = pd.merge(dyna, user, on='User ID')


def parse_time(time_in, timezone_offset_in_minute=0):
    """
    将 json 中 time_format 格式的 time 转化为 local datatime
    """
    date = datetime.strptime(time_in, '%a %b %d %H:%M:%S %z %Y')  # 这是 UTC 时间
    return date + timedelta(minutes=timezone_offset_in_minute)


new_time = []
for index, row in tqdm(dyna.iterrows()):
    date = parse_time(row['UTC time'], int(row['Timezone offset in minutes']))
    new_time.append(date.strftime('%Y-%m-%dT%H:%M:%SZ'))

dyna['time'] = new_time
dyna['type'] = 'trajectory'
dyna = dyna.rename(columns={'geo_id': 'location', 'usr_id': 'entity_id'})
dyna = dyna.reindex(columns=['dyna_id', 'type', 'time', 'entity_id', 'location'])
dyna.to_csv(output_folder + '/foursquare_nyc.dyna', index=False)

user = user.drop(['User ID'], axis=1)
user.to_csv(output_folder + '/foursquare_nyc.usr', index=False)
