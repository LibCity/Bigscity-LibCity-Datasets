# link: https://www.microsoft.com/en-us/research/publication/hidden-markov-map-matching-noise-sparseness/
import re
import os
import json
from util import ensure_dir
from datetime import datetime

network = trackInfo = truth = geo = rel = dyna = usr = dyna_route = None


def processGeoRelDyna():
    # title
    geo.write("geo_id,type,coordinates\n")
    rel.write("rel_id,type,origin_id,destination_id\n")
    dyna.write("dyna_id,type,time,entity_id,coordinates\n")

    # a dictionary, key: node_id, value: {"prev": [geo_id], "next": [geo_id]}
    rel_dct = {}
    edge_geo_dct = {}

    # read network, write geo, inner rel / save outer rel in rel_dct
    geo_id = rel_id = 0
    network.readline()
    for line in network:
        if not line:
            break

        # save outer rel
        edge_id, from_id, to_id = line.split('\t')[0:3]

        # init rel_dct
        if from_id not in rel_dct.keys():
            rel_dct[from_id] = {"prev": [], "next": []}
        if to_id not in rel_dct.keys():
            rel_dct[to_id] = {"prev": [], "next": []}

        # two way flag
        two_way = line.split('\t')[3]

        # get node list
        nodes_str = re.search("\\(.+\\)", line)[0]
        nodes_str_lst = nodes_str.replace('(', '').replace(')', '').split(', ')
        node_lst = list(map(lambda x: [eval(x.split(' ')[0]), eval(x.split(' ')[1])], nodes_str_lst))

        # write geo and inner rel
        node_i = 0

        # set rel_dct 1
        rel_dct[from_id]["next"].append(geo_id)
        edge_geo_dct[edge_id] = []
        while node_i < len(node_lst) - 1:
            geo.write(str(geo_id) + ',LineString,"' + str([node_lst[node_i], node_lst[node_i + 1]]) + '"\n')
            edge_geo_dct[edge_id].append(geo_id)
            geo_id += 1
            if node_i != len(node_lst) - 2:
                rel.write(str(rel_id) + ',geo,' + str(geo_id - 1) + ',' + str(geo_id) + '\n')
                rel_id += 1
            node_i += 1

        # set rel_dct 2
        rel_dct[to_id]["prev"].append(geo_id - 1)

        if two_way:
            node_i = 0

            # set rel_dct 3
            rel_dct[from_id]["prev"].append(geo_id)

            while node_i < len(node_lst) - 1:
                geo.write(str(geo_id) + ',LineString,"' + str([node_lst[node_i + 1], node_lst[node_i]]) + '"\n')
                geo_id += 1
                if node_i != len(node_lst) - 2:
                    rel.write(str(rel_id) + ',geo,' + str(geo_id) + ',' + str(geo_id - 1) + '\n')
                    rel_id += 1
                node_i += 1

            # set rel_dct 4
            rel_dct[to_id]["next"].append(geo_id - 1)

    # write outer rel
    for _, dct in rel_dct.items():
        for edge_i in dct["prev"]:
            for edge_j in dct["next"]:
                rel.write(str(rel_id) + ',geo,' + str(edge_i) + ',' + str(edge_j) + '\n')
                rel_id += 1

    # track
    dyna_id = 0
    trackInfo.readline()
    for line in trackInfo:
        lat = eval(line.split()[2])
        lon = eval(line.split()[3])
        date = datetime.strptime(line.split()[0] + ' ' + line.split()[1], '%d-%b-%Y %H:%M:%S')
        time = date.strftime('%Y-%m-%dT%H:%M:%SZ')
        dyna.write(str(dyna_id) + ',trajectory,' + time + ',0,"' + str([lon, lat]) + '"\n')
        dyna_id += 1

    dyna_route.write("dyna_id,type,time,entity_id,location\n")
    truth.readline()
    dyna_id = 0
    for line in truth:
        edge_id = line.split()[0]
        traversed = int(line.split()[1].strip())
        if traversed == 1:
            for geo_id in edge_geo_dct[edge_id]:
                dyna_route.write(str(dyna_id) + ',trajectory,,0,' + str(geo_id) + "\n")
                dyna_id += 1
        else:
            for geo_id in edge_geo_dct[edge_id][::-1]:
                dyna_route.write(str(dyna_id) + ',trajectory,,0,' + str(geo_id) + "\n")
                dyna_id += 1


def processUsr():
    usr.write("usr_id\n")
    usr.write("0")


def processConfig():
    config = dict()
    config['geo'] = dict()
    config['geo']['including_types'] = ['LineString']
    config['geo']['Point'] = {}
    config['usr'] = dict()
    config['usr']['properties'] = {}
    config['rel'] = dict()
    config['rel']['including_types'] = ['geo']
    config['rel']['geo'] = {}
    config['dyna'] = dict()
    config['dyna']['including_types'] = ['trajectory']
    config['dyna']['trajectory'] = {'entity_id': 'usr_id'}
    config['info'] = dict()
    config['info']['geo_file'] = 'Seattle'
    config['info']['rel_file'] = 'Seattle'
    config['info']['usr_file'] = 'Seattle'
    config['info']['dyna_file'] = 'Seattle'
    config['info']['truth_file'] = 'Seattle_truth'
    json.dump(config, open('output/Seattle/config.json', 'w', encoding='utf-8'),
              ensure_ascii=False, indent=4)


def openFile():
    global network, trackInfo, truth, geo, rel, dyna, usr, dyna_route
    input_path = './input/Seattle'
    network = open(os.path.join(input_path, "road_network.txt"), "r")
    trackInfo = open(os.path.join(input_path, "gps_data.txt"), "r")
    truth = open(os.path.join(input_path, "ground_truth_route.txt"), "r")

    outputPath = './output/Seattle'
    ensure_dir(outputPath)

    geo = open(os.path.join(outputPath, "Seattle.geo"), "w")
    rel = open(os.path.join(outputPath, "Seattle.rel"), "w")
    dyna = open(os.path.join(outputPath, "Seattle.dyna"), "w")
    usr = open(os.path.join(outputPath, "Seattle.usr"), "w")
    dyna_route = open(os.path.join(outputPath, "Seattle_truth.dyna"), "w")


def closeFile():
    global network, trackInfo, truth, geo, rel, dyna, usr, dyna_route
    network.close()
    trackInfo.close()
    truth.close()
    geo.close()
    rel.close()
    dyna.close()
    usr.close()
    dyna_route.close()


def dataTransform():
    openFile()
    processGeoRelDyna()
    processUsr()
    processConfig()
    closeFile()


if __name__ == '__main__':
    dataTransform()
