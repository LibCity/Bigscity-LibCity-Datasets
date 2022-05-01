# link: https://github.com/YibinShen/TTPNet

import json
import util
import numpy as np
import pandas as pd


output_dir = 'output/Beijing_Taxi_Sample'
util.ensure_dir(output_dir)

data_url = 'input/Beijing_Taxi_Sample/'
data_name = output_dir + '/Beijing_Taxi_Sample'

dataset_list = ["2013-10-08", "2013-10-09"] + [f"2013-10-{i}" for i in range(10, 14)] + \
               [f"2013-10-{i}" for i in range(15, 31)]

geo = []
# geo_file = np.load(data_url + 'embedding_128.npy')
geo_file = np.load('Config/' + 'embedding_128.npy')
for geo_id in range(geo_file.shape[0]):
    geo_embedding = ','.join(str(embedding) for embedding in geo_file[geo_id])
    geo.append([geo_id, 'Polygon', '[' + geo_embedding + ']'])
geo = pd.DataFrame(geo, columns=['geo_id', 'type', 'embeddings'])
geo.to_csv(data_name+'.geo', index=False)

usr = []
usr_set = set()

traj_id = 0
dyna_id = 0

dist_gap_list = []
time_gap_list = []
lngs_list = []
lats_list = []
dist_list = []
time_list = []
speeds_list = []
speeds_relevant1_list = []
speeds_relevant2_list = []
speeds_long_list = []
grid_len_list = []

dyna_file = open(data_name + '.dyna', 'w')
dyna_file.write('dyna_id' + ',' + 'type' + ',' + 'time' + ',' + 'entity_id' + ',' +
                'traj_id' + ',' + 'location' + ',' + 'coordinates' + ',' + 'current_dis' + ',' +
                'speeds' + ',' + 'speeds_relevant1' + ',' + 'speeds_relevant2' + ',' +
                'speeds_long' + ',' + 'grid_len' + ',' + 'holidays' + '\n')
for dataset in dataset_list:
    content = json.load(open(data_url + dataset + ".json", 'r'))
    for traj in content:
        time_gaps = traj["time_gap"]
        dist = traj["dist"]
        lats = traj["lats"]
        lngs = traj["lngs"]
        usr_id = traj["driverID"]
        week_id = traj["weekID"]
        time_id = traj["timeID"]
        holiday = traj["dateID"]
        time = traj["time"]
        dist_gaps = traj["dist_gap"]
        locations = traj["grid_id"]
        speeds = np.array(traj["speeds_0"]).reshape(-1, 4).tolist()
        speeds_relevant1 = np.array(traj["speeds_1"]).reshape(-1, 4).tolist()
        speeds_relevant2 = np.array(traj["speeds_2"]).reshape(-1, 4).tolist()
        speeds_long = np.array(traj["speeds_long"]).reshape(-1, 7).tolist()
        grids_len = traj["grid_len"]
        coordinates = []

        time_list.append(time)
        dist_list.append(dist)

        for lng, lat in zip(lngs, lats):

            lats_list.append(lat)
            lngs_list.append(lng)

            coordinates.append('"[' + str(lng) + ',' + str(lat) + ']"')
        if usr_id not in usr_set:
            usr_set.add(usr_id)
            usr.append([usr_id])
        start_time = util.datetime_timestamp(f'{dataset}T00:00:00Z') + time_id * 900

        last_time_gap = 0
        last_dist_gap = 0

        for time_gap, location, coordinate, dist_gap, speed, speed_relevant1, speed_relevant2, speed_long, grid_len \
            in zip(time_gaps, locations, coordinates, dist_gaps, speeds, speeds_relevant1, speeds_relevant2, speeds_long, grids_len):
            speed_s = '"[' + ",".join(str(s) for s in speed) + ']"'
            speed_relevant1_s = '"[' + ','.join(str(s) for s in speed_relevant1) + ']"'
            speed_relevant2_s = '"[' + ','.join(str(s) for s in speed_relevant2) + ']"'
            speed_long_s = '"[' + ','.join(str(s) for s in speed_long) + ']"'
            dyna_file.write(
                str(dyna_id) + ',' + 'trajectory' + ',' + str(util.timestamp_datetime(start_time + time_gap)) + ','
                + str(usr_id) + ',' + str(traj_id) + ',' + str(location) + ',' + str(coordinate) + ',' + str(dist_gap) + ','
                + speed_s + ',' + speed_relevant1_s + ',' + speed_relevant2_s + ',' + speed_long_s + ','
                + str(grid_len) + ',' + str(holiday) + '\n'
            )
            dyna_id += 1

            time_gap_list.append(time_gap - last_time_gap)
            dist_gap_list.append(dist_gap - last_dist_gap)
            last_time_gap = time_gap
            last_dist_gap = dist_gap
            speeds_list.extend(speed)
            speeds_relevant1_list.extend(speed_relevant1)
            speeds_relevant2_list.extend(speed_relevant2)
            speeds_long_list.extend(speed_long)
            grid_len_list.append(grid_len)
        traj_id += 1
    print(f"finish {dataset}")
dyna_file.close()

usr = pd.DataFrame(usr, columns=['usr_id'])
usr.to_csv(data_name + '.usr', index=False)

config = dict()
config['geo'] = dict()
config['geo']['including_types'] = ['Polygon']
config['geo']['Polygon'] = {'embedding': 'other'}
config['usr'] = dict()
config['usr']['properties'] = {}
config['dyna'] = dict()
config['dyna']['including_types'] = ['trajectory']
config['dyna']['trajectory'] = {
    'entity_id': 'usr_id',
    'traj_id': 'num',
    'coordinates': 'coordinate',
    'current_dis': 'num',
    'speeds': 'other',
    'speeds_relevant1': 'other',
    'speeds_relevant2': 'other',
    'speeds_long': 'other',
    'grid_len': 'num',
    'holiday': 'num',
}
config['info'] = dict()
config['info']['geo_file'] = 'Beijing_Taxi_Sample'
config['info']['rel_file'] = 'Beijing_Taxi_Sample'
config['info']['usr_file'] = 'Beijing_Taxi_Sample'
config['info']['dyna_file'] = 'Beijing_Taxi_Sample'
json.dump(config, open(output_dir + '/config.json', 'w', encoding='utf-8'), ensure_ascii=False)

print("dist_gap_mean: {}".format(np.mean(dist_gap_list)))
print("dist_gap_std : {}".format(np.std(dist_gap_list)))
print("time_gap_mean: {}".format(np.mean(time_gap_list)))
print("time_gap_std : {}".format(np.std(time_gap_list)))
print("lngs_mean: {}".format(np.mean(lngs_list)))
print("lngs_std : {}".format(np.std(lngs_list)))
print("lats_mean: {}".format(np.mean(lats_list)))
print("lats_std : {}".format(np.std(lats_list)))
print("dist_mean: {}".format(np.mean(dist_list)))
print("dist_std : {}".format(np.std(dist_list)))
print("time_mean: {}".format(np.mean(time_list)))
print("time_std : {}".format(np.std(time_list)))
print("speeds_mean: {}".format(np.mean(speeds_list)))
print("speeds_std : {}".format(np.std(speeds_list)))
print("speeds_relevant1_mean: {}".format(np.mean(speeds_relevant1_list)))
print("speeds_relevant1_std : {}".format(np.std(speeds_relevant1_list)))
print("speeds_relevant2_mean: {}".format(np.mean(speeds_relevant2_list)))
print("speeds_relevant2_std : {}".format(np.std(speeds_relevant2_list)))
print("speeds_long_mean: {}".format(np.mean(speeds_long_list)))
print("speeds_long_std : {}".format(np.std(speeds_long_list)))
print("grid_len_mean: {}".format(np.mean(grid_len_list)))
print("grid_len_std : {}".format(np.std(grid_len_list)))
