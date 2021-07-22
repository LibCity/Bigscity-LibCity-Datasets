import pandas as pd
from datetime import datetime
country_code = 'US' # JP
city = 'NYC'

# 尝试用逆地理编码来筛选出东京和纽约的 POI 点
# 先按 profile 筛用户

user = pd.read_csv('./dataset_UbiComp2016_UserProfile_{}.txt'.format(city), sep= '\t', encoding='ISO-8859-1', names=['User ID', 'Gender', 'Twitter friend count', 'Twitter follower count'])

check_in = pd.read_csv('./dataset_TIST2015/dataset_TIST2015_Checkins.txt', sep='\t', encoding='utf-8', names=['User ID', 'Venue ID', 'UTC time', 'Timezone offset in minutes'])

check_in_with_user = pd.merge(check_in, user, on=['User ID'])
# check_in_with_user.to_csv('./check_in_with_user_{}.csv'.format(city))

# check_in_with_user = pd.read_csv('./check_in_with_user_{}.csv'.format(city))
poi = pd.read_csv('./dataset_TIST2015/dataset_TIST2015_POIs.txt', sep= '\t', encoding='utf-8', names=['Venue ID', 'Latitude', 'Longitude', 'Venue category name', 'Country code'])

# 在上一步 check_in_with_user 中的 POI
poi_subset = pd.unique(check_in_with_user['Venue ID'])
poi_subset = pd.DataFrame(poi_subset, columns=['Venue ID'])

poi_with_user = pd.merge(poi, poi_subset, on=['Venue ID'])
# 混杂了很多其他国家的 POI 筛出
poi_with_user = poi_with_user[poi_with_user['Country code'] == country_code]

poi_with_user.to_csv('./poi_with_user_{}.csv'.format(city))
# poi_with_user = pd.read_csv('./poi_with_user_JP.csv')
city = pd.read_csv('./dataset_TIST2015/dataset_TIST2015_Cities.txt', sep= '\t', encoding='utf-8', names=['City name', 'Latitude', 'Longitude', 'Country code', 'Country name', 'City type'])

poi_tky = pd.read_csv('./poi_in_TKY.csv')
poi_tky = pd.merge(poi_tky, poi, on=['Venue ID'])
a = poi_tky.rename(columns={'Venue ID': 'geo_id', 'Venue category name': 'venue_category_name'})
a['type'] = 'Point' # 所有都是点类型
coordinates = []
for index, row in a.iterrows():
    coordinates.append('[{},{}]'.format(row['Longitude'], row['Latitude']))

# 要将 hash 的 id 转换成 int 的 id，需要记住 hash 到 int 的映射
hash_to_id = poi_tky.filter(['Venue ID'], axis = 1)
hash_to_id['geo_id'] = poi_tky.index
a['coordinates'] = coordinates
a['geo_id'] = a.index
# 删去无关的列
a = a.drop(['Latitude', 'Longitude', 'Country code'], axis=1)
a = a.reindex(columns=['geo_id', 'type', 'coordinates', 'venue_category_name'])
a.to_csv('./foursquare_nyc.geo', index=False)
# 地理位置表做完了
check_in_tky = pd.merge(check_in_with_user, poi_tky, on=['Venue ID']) # 一共有 1112156

## 处理 user 表
user_in_tky = pd.unique(check_in_tky['User ID'])
user_in_tky = pd.DataFrame(user_in_tky, columns=['User ID'])
user_in_tky = pd.merge(user_in_tky, user, on=['User ID'])
# 好用户表做完了
a = user_in_tky.rename(columns={'User ID': 'user_id', 'Gender': 'gender', 'Twitter friend count': 'twitter_friend_count', 'Twitter follower count': 'twitter_follower_count'})
user_id_to_id = user_in_tky.filter(['User ID'], axis=1)
user_id_to_id['user_id'] = user_in_tky.index
a['user_id'] = a.index
a.to_csv('./foursquare_nyc.usr', index=False)

## 最后来处理 check-in 表
# 首先将重新编码后的 user id 与 geo id 替换进来
check_in_tky = check_in_tky.drop(['Gender', 'Twitter friend count', 'Twitter follower count'], axis=1)

check_in_tky = pd.merge(check_in_tky, hash_to_id, on=['Venue ID'])
check_in_tky = pd.merge(check_in_tky, user_id_to_id, on=['User ID'])

check_in_tky = check_in_tky.drop(['User ID', 'Venue ID'], axis=1)
## 加入 dyna_id 和 type 两列
check_in_tky['dyna_id'] = check_in_tky.index
check_in_tky['type'] = 'trajectory'
new_time = []

for index, row in check_in_tky.iterrows():
    date = datetime.strptime(row['UTC time'], '%a %b %d %H:%M:%S %z %Y') # 已经转换成 UTC 时间了 
    new_time.append(date.strftime('%Y-%m-%dT%H:%M:%SZ')) # 2020-12-07T02:59:46

check_in_tky['time'] = new_time
check_in_tky = check_in_tky.drop(['UTC time'], axis=1)
check_in_tky = check_in_tky.rename(columns={'user_id': 'entity_id'})
check_in_tky = check_in_tky.reindex(columns=['dyna_id', 'type', 'time', 'entity_id', 'geo_id', 'Timezone offset in minutes'])
check_in_tky = check_in_tky.sort_values(by='time')
check_in_tky = check_in_tky.reset_index(drop=True)
check_in_tky['dyna_id'] = check_in_tky.index

check_in_tky.to_csv('./foursquare_nyc.dyna', index=False)
