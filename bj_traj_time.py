import pickle
import csv
import os
from util import int_to_isoformat

geo_cnt = 0


def get_dyna(name):
    f = open(os.path.join("input", name), "rb")
    x = pickle.load(f)

    dyna_cnt = 0
    dyna_file = open("output/" + name + ".dyna", "w", newline='')
    dyna_writer = csv.writer(dyna_file)
    dyna_writer.writerow(["dyna_id", "type", "time", "entity_id", "location"])

    geo_cnt = 0
    geos = {}
    geo_file = open("output/" + name + ".geo", "w", newline='')
    geo_writer = csv.writer(geo_file)
    geo_writer.writerow(["geo_id", "type", "coordinates"])

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
        for time, coords in path:
            cur_time = int_to_isoformat(time)
            coords = (coords[1], coords[0])  # list to tuple

            dyna_cnt += 1
            dyna_col = [dyna_cnt, "trajectory", cur_time,
                        entity_id,
                        get_geo_id(coords)]
            dyna_writer.writerow(dyna_col)
        break


def convert(x):
    if x <= 10:
        return "0" + str(x)
    return str(x)


month = 6
for day in range(1, 32):
    file_name = "gps_2015" + convert(month) + convert(day)
    if os.path.exists(os.path.join("input", file_name)):
        get_dyna(file_name)
