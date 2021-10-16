from zipfile import ZipFile
import re
import os
import csv
from util import int_to_isoformat

pattern = re.compile(r"(\S+),(\S+),\"\[(.+)]\"\n")
detail_pattern = re.compile(r"(\S+) (\S+) (\S+), ")
geo_cnt = 0


def get_dyna(file, name, binary):
    wrong_columns = []

    dyna_cnt = 0
    dyna_file = open(os.path.join("output", name + ".dyna"), "w", newline='')
    dyna_writer = csv.writer(dyna_file)
    dyna_writer.writerow(["dyna_id", "type", "time", "entity_id", "traj_id", "location"])

    geo_cnt = 0
    geos = {}
    geo_file = open(os.path.join("output", name + ".geo"), "w", newline='')
    geo_writer = csv.writer(geo_file)
    geo_writer.writerow(["geo_id", "type", "coordinates"])

    ids = {}
    cur_id = 0
    usr_file = open(os.path.join("output", name + ".usr"), "w", newline='')
    usr_writer = csv.writer(usr_file)
    usr_writer.writerow(["usr_id"])

    def get_geo_id(coordinates):
        global geo_cnt
        if coordinates not in geos:
            geo_cnt += 1
            geos[coordinates] = geo_cnt
            geo_writer.writerow([geo_cnt, "Point", [coordinates[0], coordinates[1]]])
        return geos[coordinates]

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
                coords = (float(longitude), float(latitude))
                dyna_cnt += 1
                dyna_col = [dyna_cnt, "trajectory", cur_time,
                            entity_id, traj_id,
                            get_geo_id(coords)]
                dyna_writer.writerow(dyna_col)
        else:
            wrong_columns.append(line)
    if len(wrong_columns) > 0:
        print("wrong_columns in " + name + ":")
        print(wrong_columns)
    print("finished " + name)
    print()


filenames = ["chengdushi_1101_1110", "chengdushi_1110_1120", "chengdushi_1120_1130"]
for filename in filenames:
    f = None
    if os.path.exists(os.path.join("input", filename + ".zip")):
        myzip = ZipFile(os.path.join("input", filename + ".zip"))
        f = myzip.open(filename + ".csv")
        get_dyna(f, filename, binary=True)
    elif os.path.exists(os.path.join("input", filename + ".csv")):
        f = open(os.path.join("input", filename + ".csv"))
        get_dyna(f, filename, binary=False)
