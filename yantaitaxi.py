# link: http://data.sd.gov.cn/cmpt/cmptDetail.html?id=65

import util
import json
import pandas as pd


output_dir = 'output/YantaiTaxi'
util.ensure_dir(output_dir)

data_url = 'input/YantaiTaxi/'
output_name = output_dir + '/YantaiTaxi'

dataset_list = ["taxi_gps_0809.csv", "taxi_gps_10.csv", "taxi_gps_1011.csv", "taxi_gps_12.csv"]
usr_id = 0
usr_dict = dict()
device_sn_dict = dict()
dyna_id = 0

dyna_file = open(output_name + '.dyna', 'w')
dyna_file.write(
    'dyna_id' + ',' + 'type' + ',' + 'time' + ',' + 'entity_id' + ',' +
    'traj_id' + ',' + 'coordinates' + ',' + 'current_spd' + '\n'
)
for dataset in dataset_list:
    content = pd.read_csv(data_url + dataset)
    for index, row in content.iterrows():
        device_sn = row["DeviceSN"]
        # sim_num = row["SIMNum"]
        if device_sn not in device_sn_dict:
            device_sn_dict[device_sn] = usr_id
            usr_dict[usr_id] = [device_sn]
            # usr_dict[usr_id] = [device_sn, sim_num]
            usr_id += 1
        entity_id = device_sn_dict[device_sn]
        # assert usr_dict[entity_id][1] == sim_num, f"{entity_id} usr_dict: {usr_dict[entity_id][1]}, sim_num: {sim_num}"
        time_id = row["GPS_Date"].split()
        time_id = time_id[0] + 'T' + time_id[1] + "Z"
        lng, lat, speed = row["LonVar"], row["LatVar"], row["SpeedVar"]
        coordinate = '"[' + str(lng) + ',' + str(lat) + ']"'
        dyna_file.write(
            str(dyna_id) + ',' + 'trajectory' + ',' + time_id + ',' + str(entity_id) + ',' +
            '0' + ',' + coordinate + ',' + str(speed) + '\n'
        )
        dyna_id += 1
        if index % 100000 == 0:
            print(f"finish {dataset} {index}")
dyna_file.close()

usr = []
for i in range(usr_id):
    usr.append([i, usr_dict[i][0]])
    # usr.append([i, usr_dict[i][0], usr_dict[i][1]])
usr = pd.DataFrame(usr, columns=['usr_id', 'DeviceSN'])
# usr = pd.DataFrame(usr, columns=['usr_id', 'DeviceSN', 'SIMNum'])
usr.to_csv(output_name + ".usr", index=False)

config = dict()
config['usr'] = dict()
config['usr']['properties'] = {'DeviceSN': 'other'}
config['dyna'] = dict()
config['dyna']['including_types'] = ['trajectory']
config['dyna']['trajectory'] = {'entity_id': 'usr_id', 'traj_id': 'num', 'coordinates': 'coordinate', 'current_spd': 'num'}
config['info'] = dict()
config['info']["need_cut"] = True
json.dump(config, open(output_dir + '/config.json', 'w', encoding='utf-8'), ensure_ascii=False)
