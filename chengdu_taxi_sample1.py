# link: https://github.com/UrbComp/DeepTTE/tree/master/data
import json
import util
import numpy as np
import pandas as pd


output_dir = 'output/Chengdu_Taxi_Sample1'
util.ensure_dir(output_dir)

data_url = 'input/Chengdu_Taxi_Sample1/'
data_name = output_dir + '/Chengdu_Taxi_Sample1'

dataset_list = ["train_00", "train_01", "train_02", "train_03", "train_04", "test"]
# geo = []
# geo_dict = dict()
# geo_id = 0
usr = []
usr_set = set()
traj_id = 0
dyna_id = 0

lats_list = []
lngs_list = []
time_list = []
dist_list = []
time_gap_list = []
dist_gap_list = []

dyna_file = open(data_name + '.dyna', 'w')
dyna_file.write('dyna_id' + ',' + 'type' + ',' + 'time' + ',' + 'entity_id' + ',' +
                'traj_id' + ',' + 'coordinates' + ',' + 'current_dis' + ',' + 'current_state' + '\n')
for dataset in dataset_list:
    content = open(data_url + dataset, "r").readlines()
    for line in content:
        traj = json.loads(line)
        time_gaps = traj["time_gap"]
        dist = traj["dist"]
        lats = traj["lats"]
        usr_id = traj["driverID"]
        week_id = traj["weekID"]
        states = traj["states"]
        time_id = traj["timeID"]
        date_id = traj["dateID"]
        time = traj["time"]
        lngs = traj["lngs"]
        dist_gaps = traj["dist_gap"]        
        time_id = traj["timeID"]
        # locations = []
        coordinates = []

        time_list.append(time)
        dist_list.append(dist)

        for lng, lat in zip(lngs, lats):
            
            lats_list.append(lat)
            lngs_list.append(lng)
            
            coordinates.append('"[' + str(lng) + ',' + str(lat) + ']"')
            # if (lng, lat) not in geo_dict:
            #     geo_dict[(lng, lat)] = geo_id
            #     geo.append([geo_id, 'Point', '[' + str(lng) + ', ' + str(lat) + ']'])
            #     geo_id += 1
            # locations.append(geo_dict[(lng, lat)])
        if usr_id not in usr_set:
            usr_set.add(usr_id)
            usr.append([usr_id])
        start_time = util.datetime_timestamp(f'2014-08-{date_id}T00:00:00Z') + time_id * 60

        last_time_gap = 0
        last_dist_gap = 0

        # for time_gap, dist_gap, location, state in zip(time_gaps, dist_gaps, locations, states):
        #     dyna_file.write(str(dyna_id) + ',' + 'trajectory' + ',' + str(util.timestamp_datetime(start_time + time_gap)) + ','
        #     + str(usr_id) + ',' + str(traj_id) + ',' + str(location) + ',' + str(dist_gap) + ',' + str(state) + '\n')
        for time_gap, dist_gap, coordinate, state in zip(time_gaps, dist_gaps, coordinates, states):
            dyna_file.write(str(dyna_id) + ',' + 'trajectory' + ',' + str(util.timestamp_datetime(start_time + time_gap)) + ','
            + str(usr_id) + ',' + str(traj_id) + ',' + str(coordinate) + ',' + str(dist_gap) + ',' + str(state) + '\n')
            dyna_id += 1

            time_gap_list.append(time_gap - last_time_gap)
            dist_gap_list.append(dist_gap - last_dist_gap)
            last_time_gap = time_gap
            last_dist_gap = dist_gap
        traj_id += 1
dyna_file.close()
# geo = pd.DataFrame(geo, columns=['geo_id', 'type', 'coordinates'])
# geo.to_csv(data_name + '.geo', index=False)
usr = pd.DataFrame(usr, columns=['usr_id'])
usr.to_csv(data_name + '.usr', index=False)

config = dict()
# config['geo'] = dict()
# config['geo']['including_types'] = ['Point']
# config['geo']['Point'] = {}
config['usr'] = dict()
config['usr']['properties'] = {}
config['dyna'] = dict()
config['dyna']['including_types'] = ['trajectory']
config['dyna']['trajectory'] = {'entity_id': 'usr_id', 'traj_id': 'num', 'coordinates': 'coordinate', 'current_dis': 'num', 'current_state': 'num'}
config['info'] = dict()
config['info']['usr_file'] = 'Chengdu-Taxi-Sample1'
config['info']['dyna_file'] = 'Chengdu-Taxi-Sample1'
json.dump(config, open(output_dir + '/config.json', 'w', encoding='utf-8'), ensure_ascii=False)

print("lngs_mean: {}".format(np.mean(lngs_list)))
print("lngs_std : {}".format(np.std(lngs_list)))
print("lats_mean: {}".format(np.mean(lats_list)))
print("lats_std : {}".format(np.std(lats_list)))
print("time_mean: {}".format(np.mean(time_list)))
print("time_std : {}".format(np.std(time_list)))
print("dist_mean: {}".format(np.mean(dist_list)))
print("dist_std : {}".format(np.std(dist_list)))
print("time_gap_mean: {}".format(np.mean(time_gap_list)))
print("time_gap_std : {}".format(np.std(time_gap_list)))
print("dist_gap_mean: {}".format(np.mean(dist_gap_list)))
print("dist_gap_std : {}".format(np.std(dist_gap_list)))
