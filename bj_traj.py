# link: Please contact the author!
import pickle
import csv
import os
import json
from util import int_to_isoformat, ensure_dir


def convert(x):
    if x <= 10:
        return "0" + str(x)
    return str(x)


month = 6  # ONLY CHANGE HERE IF U WANT OTHER MONTHS' DATA
DATA_NAME = "bj_traj_2015" + convert(month)
geo_cnt = 0
output_dir = os.path.join("output", DATA_NAME)
ensure_dir(output_dir)

dyna_cnt = 0
dyna_file = open(os.path.join(output_dir, DATA_NAME + ".dyna"), "w", newline='')
dyna_writer = csv.writer(dyna_file)
dyna_writer.writerow(["dyna_id", "type", "time", "entity_id", "location"])

usr_file = open(os.path.join(output_dir, DATA_NAME + ".usr"), "w", newline='')
usr_writer = csv.writer(usr_file)
usr_writer.writerow(["usr_id"])

entity_id = 0


def dumpconfig(data_name):
    config = dict()
    config['usr'] = dict()
    config['usr']['properties'] = dict()
    config['dyna'] = dict()
    config['dyna']['including_types'] = ['trajectory']
    config['dyna']['trajectory'] = {'entity_id': 'usr_id',
                                    'location': 'coordinate'}
    json.dump(config, open(os.path.join(data_name, 'config.json'),
                           'w', encoding='utf-8'), ensure_ascii=False)


def get_dyna(f):
    global entity_id, dyna_cnt
    x = pickle.load(f)

    for path in x:
        entity_id += 1
        usr_writer.writerow([entity_id])
        for time, coords in path:
            cur_time = int_to_isoformat(time)
            coords = [coords[1], coords[0]]

            dyna_cnt += 1
            dyna_col = [dyna_cnt, "trajectory", cur_time,
                        entity_id, coords]
            dyna_writer.writerow(dyna_col)


for day in range(1, 32):
    time = "2015" + convert(month) + convert(day)
    file_name = "gps_" + time
    input_dirname = os.path.join("input", DATA_NAME)
    input_filename = os.path.join(input_dirname, file_name)
    if os.path.exists(input_filename):
        file = open(input_filename, "rb")
        get_dyna(file)

dumpconfig(output_dir)