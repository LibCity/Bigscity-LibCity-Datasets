import pickle
import csv
import os
from util import int_to_isoformat, ensure_dir

geo_cnt = 0


def get_dyna(f, name):
    x = pickle.load(f)

    output_dir = os.path.join("output", name)
    ensure_dir(output_dir)

    dyna_cnt = 0
    dyna_file = open(os.path.join(output_dir, name + ".dyna"), "w", newline='')
    dyna_writer = csv.writer(dyna_file)
    dyna_writer.writerow(["dyna_id", "type", "time", "entity_id", "location"])

    geo_cnt = 0
    geos = {}
    geo_file = open(os.path.join(output_dir, name + ".geo"), "w", newline='')
    geo_writer = csv.writer(geo_file)
    geo_writer.writerow(["geo_id", "type", "coordinates"])

    usr_file = open(os.path.join(output_dir, name + ".usr"), "w", newline='')
    usr_writer = csv.writer(usr_file)
    usr_writer.writerow(["usr_id"])

    def get_geo_id(coordinates):
        global geo_cnt
        if coordinates not in geos:
            geo_cnt += 1
            geos[coordinates] = geo_cnt
            geo_writer.writerow([geo_cnt, "Point", [coordinates[0], coordinates[1]]])
        return geos[coordinates]

    entity_id = 0
    for path in x:
        entity_id += 1
        usr_writer.writerow([entity_id])
        for time, coords in path:
            cur_time = int_to_isoformat(time)
            coords = (coords[1], coords[0])  # list to tuple

            dyna_cnt += 1
            dyna_col = [dyna_cnt, "trajectory", cur_time,
                        entity_id,
                        get_geo_id(coords)]
            dyna_writer.writerow(dyna_col)


def convert(x):
    if x <= 10:
        return "0" + str(x)
    return str(x)


DATA_NAME = "bj_traj"
month = 6
for day in range(1, 32):
    time = "2015" + convert(month) + convert(day)
    file_name = "gps_" + time
    input_dirname = os.path.join("input", DATA_NAME)
    input_filename = os.path.join(input_dirname, file_name)
    if os.path.exists(input_filename):
        file = open(input_filename, "rb")
        get_dyna(file, DATA_NAME + "_" + time)
