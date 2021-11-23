#link https://dmis.korea.ac.kr/cape
import os
import pandas as pd
from datetime import datetime

output_folder = './output'
if not os.path.exists(output_folder):
    os.makedirs(output_folder)
# poi_index = pd.read_csv('./poi_index.txt', sep='\t', names=['index', 'poi'])  # 应该是 index 和 poi name
word_index = pd.read_csv('./input/word_index.txt', sep='\t', names=['index', 'word'])

# poi 是 id， text_content 是由 word index 组成的句子，啊这我要给他还原吗，还是给他还原？
# 还原吧
train = pd.read_csv('./input/train.txt', sep='\t', names=['user', 'lat', 'lng', 'timestamp', 'poi', 'text_content'])
test = pd.read_csv('./input/test.txt', sep='\t', names=['user', 'lat', 'lng', 'timestamp', 'poi', 'text_content'])
validation = pd.read_csv('./input/validation.txt', sep='\t', names=['user', 'lat', 'lng', 'timestamp', 'poi', 'text_content'])
# 把 train test validation 拼接起来吧
total_check_in = pd.concat([train, test, validation])

# # 先来做 geo 表
poi_info = total_check_in.filter(items=['lat', 'lng', 'poi'])
poi_info = poi_info.groupby('location_id').mean()
poi_info.reset_index(inplace=True)
# 引入 poi name 信息
poi = poi_info.merge(poi_index, left_on='poi', right_on='index')
poi['type'] = 'Point'

# 计算 coordinates
coordinates = []
for index, row in poi.iterrows():
    coordinates.append('[{},{}]'.format(row['lng'], row['lat']))

poi['coordinates'] = coordinates
poi = poi.rename(columns={'poi_x': 'geo_id', 'poi_y': 'poi_name'})
poi = poi.drop(['lat', 'lng', 'index'])
poi = poi.reindex(columns=['geo_id', 'type', 'coordinates', 'poi_name'])
poi.to_csv(output_folder + '/instagram.geo', index=False)

# 做 usr 表
user = pd.unique(total_check_in['user'])
user = pd.DataFrame(user, columns=['usr_id'])
user.to_csv(output_folder + '/instagram.usr', index=False)

# 做 dyna 表
# 需要首先对时间做转换
time = []
text = []
# 居然有 text 为空的，建议删除 760703
total_check_in = total_check_in.dropna(axis=0, how='any')
total_check_in = total_check_in[:10]
for index, row in total_check_in.iterrows():
    date = datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S')
    time.append(date.strftime('%Y-%m-%dT%H:%M:%SZ'))  # 2020-12-07T02:59:46
    word_index_list = row['text_content'].split(' ')
    words = ''
    for w in word_index_list:
        try:
            words = words + word_index.iloc[int(w)]['word'] + ' '
        except TypeError:
            words += ' '
    # 最后一个是空格
    text.append(words[:-1])

total_check_in['time'] = time
total_check_in['text'] = text
total_check_in = total_check_in.drop(['lat', 'lng', 'timestamp', 'text_content'], axis=1)
total_check_in = total_check_in.rename(columns={'poi': 'location', 'user': 'entity_id'})
total_check_in['type'] = 'traj'
total_check_in = total_check_in.sort_values(by='time')
total_check_in = total_check_in.reset_index(drop=True)
total_check_in['dyna_id'] = total_check_in.index
total_check_in = total_check_in.reindex(columns=['dyna_id', 'type', 'time', 'entity_id', 'location', 'text'])
total_check_in.to_csv(output_folder + '/instagram.dyna', index=False)
