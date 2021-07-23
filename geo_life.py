#link: http://research.microsoft.com/en-us/projects/geolife/default.aspx
import os
import pandas as pd
from datetime import datetime
'''
geo-life 被拆分成两个表
usr: 只包含所有 usr_id
dyna: dyna_id, type, time, entity_id, location(coordinate), altitude(feet), traj_id, transportation_mode 
'''
output_folder = './output'
if not os.path.exists(output_folder):
    os.makedirs(output_folder)
path = os.getcwd()
data_path = os.path.join(path, './input/data')

usr_set = os.listdir(data_path)
usr_set.sort()
usr = [] # 用来构建 usr 表
traj_id = 0
verbose = 20
dyna = pd.DataFrame(columns=['time', 'entity_id', 'location', 'altitude', 'traj_id', 'transportation_mode'])

for uid in usr_set:
    usr.append(int(uid))
    sub_dir = os.listdir(os.path.join(data_path, uid))
    has_labels = True
    labels = []
    if 'labels.txt' not in sub_dir:
        has_labels = False
    else:
        # 加载 label 文件
        df = pd.read_csv(os.path.join(data_path, uid, 'labels.txt'), sep='\t')
        for index, row in df.iterrows():
            start_time = row['Start Time']
            start_time = datetime.strptime(start_time, '%Y/%m/%d %H:%M:%S')
            labels.append([start_time, row['Transportation Mode']])
    # 下面来处理轨迹
    index_labels = 0 # 因为都是按时间顺序排序的，所以我们只有遍历一遍就能知道某条轨迹是否有对应的 trans_mode
    traj_dir = os.path.join(data_path, uid, 'Trajectory')
    traj_files = os.listdir(traj_dir)
    traj_files.sort()
    for traj_file in traj_files:
        traj_start_time = traj_file.split('.')[0]
        traj_start_time = datetime.strptime(traj_start_time, '%Y%m%d%H%M%S')
        trans_mode = 'Missing'
        if has_labels:
            while index_labels < len(labels):
                label_time = labels[index_labels][0]
                delta = traj_start_time - label_time
                delta_min = delta.days * 24 * 60 + delta.seconds / 60
                if -5 <= delta_min <= 5:
                    # 因为他的时间不是完全对齐的，我也不知道为啥。所以设置一个前后 5 分钟的容错，应该不会有问题
                    trans_mode = labels[index_labels][1]
                    index_labels += 1
                    break
                elif delta_min < -5:
                    # 那么之后的 label 的 time 一定比当前的大，就会负的更多
                    break
                else:
                    # 还有匹配到的机会
                    index_labels += 1
        # 好现在正式开始转换 traj
        raw_traj = pd.read_csv(os.path.join(traj_dir, traj_file), sep=',', skiprows=6, names=['latitude', 'longitude', 'useless', 'altitude', 'date1', 'date2', 'timestamp'])
        # 计算 coordinate 和 time
        coordinates = []
        new_time = []
        for index, row in raw_traj.iterrows():
            coordinates.append('[{},{}]'.format(row['longitude'], row['latitude']))
            date = datetime.strptime(row['date2']+row['timestamp'], '%Y-%m-%d%H:%M:%S')
            new_time.append(date.strftime('%Y-%m-%dT%H:%M:%SZ'))
        raw_traj['location'] = coordinates
        raw_traj['time'] = new_time
        raw_traj['entity_id'] = int(uid)
        raw_traj['traj_id'] = traj_id
        traj_id += 1
        raw_traj['transportation_mode'] = trans_mode
        raw_traj = raw_traj.reindex(columns=['time', 'entity_id', 'location', 'altitude', 'traj_id', 'transportation_mode'])
        # 将处理好的 raw_traj append 到 dyna 中
        dyna = pd.concat([dyna, raw_traj])
        if traj_id % verbose == 0:
            print('finish 20 traj ', traj_id)
    # for test
    print('finish uid ', uid)

dyna = dyna.reset_index(drop=True)
dyna['dyna_id'] = dyna.index
dyna = dyna.reindex(columns=['dyna_id', 'type', 'time', 'entity_id', 'location', 'altitude', 'traj_id', 'transportation_mode'])
dyna.to_csv(output_folder + '/geo_life.dyna', index=False)
usr = pd.DataFrame(usr, columns=['usr_id'])
usr.to_csv(output_folder + '/geo_life.usr', index=False)
