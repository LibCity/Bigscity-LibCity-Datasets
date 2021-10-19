# link: https://zenodo.org/record/57731#.YSJRbGczZPZ
import os
import json
from util import ensure_dir
import pandas as pd

arcs = nodes = track = geo = dyna = rel = usr = route = dyna_route = None


def process(name):

    # geo
    geo.write("geo_id,type,coordinates\n")
    nodes_df = pd.read_csv(nodes, sep='\t', header=None)
    arcs_df = pd.read_csv(arcs, sep='\t', header=None)
    rel_dct = {}  # a dictionary, key: node_id, value: {"prev": [geo_id], "next": [geo_id]}
    for geo_id, edge in arcs_df.iterrows():
        # init rel_dct
        if edge[0] not in rel_dct.keys():
            rel_dct[edge[0]] = {"next": [], "prev": []}
        if edge[1] not in rel_dct.keys():
            rel_dct[edge[1]] = {"next": [], "prev": []}

        # set rel_dct
        rel_dct[edge[0]]["next"].append(geo_id)
        rel_dct[edge[1]]["prev"].append(geo_id)

        # get coordinate
        lon1 = nodes_df.loc[edge[0]][0]
        lat1 = nodes_df.loc[edge[0]][1]
        lon2 = nodes_df.loc[edge[1]][0]
        lat2 = nodes_df.loc[edge[1]][1]

        # write geo
        geo.write(str(geo_id) + ',LineString,"' + str([[lon1, lat1], [lon2, lat2]]) + '"\n')

    # rel
    rel.write("rel_id,type,origin_id,destination_id\n")
    rel_id = 0
    for _, dct in rel_dct.items():
        for edge_i in dct["prev"]:
            for edge_j in dct["next"]:
                rel.write(str(rel_id) + ',geo,' + str(edge_i) + ',' + str(edge_j) + "\n")
                rel_id += 1
    # dyna
    dyna_df = pd.read_csv(track, sep='\t', header=None)
    dyna.write("dyna_id,type,time,entity_id,coordinates\n")
    for dyna_id, row in dyna_df.iterrows():
        lon, lat = float(row[0]), float(row[1])
        dyna.write(str(dyna_id) + ',trajectory,' + ',0,"' + str([lon, lat]) + '"\n')

    # route
    dyna_route_df = pd.read_csv(route, sep='\t', header=None)
    dyna_route.write("dyna_id,type,time,entity_id,location\n")
    for dyna_id, row in dyna_route_df.iterrows():
        dyna_route.write(str(dyna_id) + ',trajectory,' + ',0,' + str(row[0]) + "\n")

def processUsr():
    usr.write("usr_id\n")
    usr.write("0")

def processConfig(name):
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
    config['info']['with_time'] = False
    config['info']['geo_file'] = name
    config['info']['rel_file'] = name
    config['info']['usr_file'] = name
    config['info']['dyna_file'] = name
    config['info']['truth_file'] = name + '_truth'
    json.dump(config, open(os.path.join('output', 'global', name, 'config.json'), 'w', encoding='utf-8'),
              ensure_ascii=False, indent=4)


def openFile(name):
    global arcs, nodes, track, geo, dyna, rel, usr, config, route, dyna_route

    input_path = os.path.join('input', 'global', name)
    arcs = open(os.path.join(input_path, name + ".arcs"), "r")
    nodes = open(os.path.join(input_path, name + ".nodes"), "r")
    track = open(os.path.join(input_path, name + ".track"), "r")
    route = open(os.path.join(input_path, name + ".route"), "r")

    output_path = os.path.join('output', 'global', name)
    ensure_dir(output_path)
    geo = open(os.path.join(output_path, name + ".geo"), "w")
    dyna = open(os.path.join(output_path, name + ".dyna"), "w")
    rel = open(os.path.join(output_path, name + ".rel"), "w")
    usr = open(os.path.join(output_path, name + ".usr"), "w")
    dyna_route = open(os.path.join(output_path, name + "_truth.dyna"), "w")


def closeFile():
    global arcs, nodes, track, geo, dyna, rel, usr, route, dyna_route
    arcs.close()
    nodes.close()
    track.close()
    geo.close()
    dyna.close()
    rel.close()
    usr.close()
    route.close()
    dyna_route.close()


def dataTransform(name):
    openFile(name)
    processConfig(name)
    process(name)
    processUsr()
    closeFile()


if __name__ == '__main__':
    for i in range(100):
        print(i)
        if i < 10:
            name = "0000000" + str(i)
        else:
            name = "000000" + str(i)
        dataTransform(name)
