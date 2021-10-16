# link: https://zenodo.org/record/57731#.YSJRbGczZPZ
import os
import json
from util import ensure_dir

arcs = nodes = track = geo = dyna = rel = usr = route_unprocessed = route_processed = None


def processGeo():
    geo.write("geo_id, type, coordinate\n")
    i = 0
    node = nodes.readline()[0:-1]
    while node != "":
        node1 = node.split("\t")[0]
        node2 = node.split("\t")[1]
        geo.write(str(i) + ',Point,"[' + node1 + ',' + node2 + ']"\n')
        i += 1
        node = nodes.readline()[0:-1]
    nodeNum = i
    node = track.readline()[0:-1]
    while node != "":
        node1 = node.split("\t")[0]
        node2 = node.split("\t")[1]
        geo.write(str(i) + ',Point,"[' + node1 + ',' + node2 + ']"\n')
        i += 1
        node = track.readline()[0:-1]
    return nodeNum


def processUsr():
    usr.write("usr_id\n")
    usr.write("0")


def processRel():
    rel.write("rel_id,type,origin_id,destination_id\n")
    i = 0
    arc = arcs.readline()[0:-1]
    while arc != "":
        arc = arc.replace('\t', ',')
        rel.write(str(i) + ',geo,' + arc + '\n')
        i += 1
        arc = arcs.readline()[0: -1]


def processDyna(nodeNum):
    dyna.write("dyna_id,type,time,entity_id,location\n")
    i = 0
    track.seek(0)
    node = track.readline()[0: -1]
    while node != "":
        dyna.write(str(i) + ',trajectory,' + ',0,' + str(i + nodeNum) + '\n')
        i += 1
        node = track.readline()[0: -1]


def processRoute():
    route_processed.write("route_id,usr_id,rel_id\n")
    line = route_unprocessed.readline()
    route_id = 0
    while line != "":
        route_processed.write(str(route_id) + ',0,' + line)
        route_id += 1
        line = route_unprocessed.readline()


def processConfig(name):
    config = dict()
    config['geo'] = dict()
    config['geo']['including_types'] = ['Point']
    config['geo']['Point'] = {}
    config['usr'] = dict()
    config['usr']['properties'] = {}
    config['rel'] = dict()
    config['rel']['including_types'] = ['geo']
    config['rel']['geo'] = {}
    config['dyna'] = dict()
    config['dyna']['including_types'] = ['trajectory']
    config['dyna']['trajectory'] = {'entity_id': 'usr_id', 'location': 'geo_id'}
    config['info'] = dict()
    config['info']['with_time'] = False
    json.dump(config, open(os.path.join('output', 'global', name, 'config.json'), 'w', encoding='utf-8'),
              ensure_ascii=False, indent=4)


def openFile(name):
    global arcs, nodes, track, geo, dyna, rel, usr, config, route_unprocessed, route_processed

    input_path = os.path.join('input', 'global', name)
    arcs = open(os.path.join(input_path, name + ".arcs"), "r")
    nodes = open(os.path.join(input_path, name + ".nodes"), "r")
    track = open(os.path.join(input_path, name + ".track"), "r")
    route_unprocessed = open(os.path.join(input_path, name + ".route"), "r")

    output_path = os.path.join('output', 'global', name)
    ensure_dir(output_path)
    geo = open(os.path.join(output_path, name + ".geo"), "w")
    dyna = open(os.path.join(output_path, name + ".dyna"), "w")
    rel = open(os.path.join(output_path, name + ".rel"), "w")
    usr = open(os.path.join(output_path, name + ".usr"), "w")
    route_processed = open(os.path.join(output_path, name + ".route"), "w")


def closeFile():
    global arcs, nodes, track, geo, dyna, rel, usr, route_unprocessed, route_processed
    arcs.close()
    nodes.close()
    track.close()
    geo.close()
    dyna.close()
    rel.close()
    usr.close()
    route_unprocessed.close()
    route_processed.close()


def dataTransform(name):
    openFile(name)
    nodeNum = processGeo()
    processUsr()
    processRel()
    processDyna(nodeNum)
    processRoute()
    processConfig(name)
    closeFile()


if __name__ == '__main__':
    for i in range(100):
        if i < 10:
            name = "0000000" + str(i)
        else:
            name = "000000" + str(i)
        dataTransform(name)
