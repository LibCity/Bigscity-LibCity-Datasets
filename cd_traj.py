from zipfile import ZipFile
import re
import os
import csv
import json
from util import int_to_isoformat, ensure_dir

pattern = re.compile(r"(\S+),(\S+),\"\[(.+)]\"\n")
detail_pattern = re.compile(r"(\S+) (\S+) (\S+), ")
geo_cnt = 0


def dumpconfig(data_name):
    config = dict()
    config['usr'] = dict()
    config['usr']['properties'] = dict()
    config['dyna'] = dict()
    config['dyna']['including_types'] = ['trajectory']
    config['dyna']['trajectory'] = {'entity_id': 'usr_id',
                                    'location': 'geo_id',
                                    'traj_id': 'coordinate'}
    json.dump(config, open(os.path.join(data_name, 'config.json'),
                           'w', encoding='utf-8'), ensure_ascii=False)


def get_dyna(file, name, binary):
    wrong_columns = []

    output_dir = os.path.join("output", name)
    ensure_dir(output_dir)

    dyna_cnt = 0
    dyna_file = open(os.path.join(output_dir, name + ".dyna"), "w", newline='')
    dyna_writer = csv.writer(dyna_file)
    dyna_writer.writerow(["dyna_id", "type", "time", "entity_id", "traj_id", "location"])

    ids = {}
    cur_id = 0
    usr_file = open(os.path.join(output_dir, name + ".usr"), "w", newline='')
    usr_writer = csv.writer(usr_file)
    usr_writer.writerow(["usr_id"])

    for line in file:
        if binary:
            line = line.decode('ascii')
        match = pattern.match(line)
        if match:
            groups = match.groups()
            customer_id = groups[0]  # unused temporarily
            driver_id = groups[1]
            position_list = groups[2] + ", "

            # get entity_id, traj_id
            if driver_id not in ids:
                cur_id += 1
                ids[driver_id] = (cur_id, 0)
                usr_writer.writerow([cur_id])
            ids[driver_id] = (ids[driver_id][0], ids[driver_id][1] + 1)
            entity_id, traj_id = ids[driver_id]

            # get dyna
            positions = re.findall(detail_pattern, position_list)
            for detail in positions:
                longitude, latitude, time = detail
                cur_time = int_to_isoformat(int(time))
                coords = [float(longitude), float(latitude)]
                dyna_cnt += 1
                dyna_col = [dyna_cnt, "trajectory", cur_time,
                            entity_id, traj_id, coords]
                dyna_writer.writerow(dyna_col)
        else:
            wrong_columns.append(line)
    dumpconfig(output_dir)
    if len(wrong_columns) > 0:
        print("wrong_columns in " + name + ":")
        print(wrong_columns)
    print("finished " + name)
    print()


def get_dynas(filenames, DATA_NAME="cd_traj"):
    for filename in filenames:
        time = filename.lstrip("abcdefghijklmnopqrstuvwxyz")
        input_dir = os.path.join("input", DATA_NAME)
        if os.path.exists(os.path.join(input_dir, filename + ".zip")):
            myzip = ZipFile(os.path.join(input_dir, filename + ".zip"))
            f = myzip.open(filename + ".csv")
            get_dyna(f, DATA_NAME + time, binary=True)
        elif os.path.exists(os.path.join(input_dir, filename + ".csv")):
            f = open(os.path.join(input_dir, filename + ".csv"))
            get_dyna(f, DATA_NAME + time, binary=False)


DATA_NAME = "cd_traj"
get_dynas(["chengdushi_1101_1110", "chengdushi_1110_1120", "chengdushi_1120_1130"], DATA_NAME)
