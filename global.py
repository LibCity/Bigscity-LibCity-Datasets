# link: https://zenodo.org/record/57731#.YSJRbGczZPZ
import os
import json

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
    route_processed.write("rel_id\n")
    line = route_unprocessed.readline()
    while line != "":
        route_processed.write(line)
        line = route_unprocessed.readline()


def processConfig():
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
    json.dump(config, open('config.json', 'w', encoding='utf-8'), ensure_ascii=False)


def openFile(name):
    global arcs, nodes, track, geo, dyna, rel, usr, config, route_unprocessed, route_processed
    os.chdir("input")
    os.chdir(name)
    arcs = open(name + ".arcs", "r")
    nodes = open(name + ".nodes", "r")
    track = open(name + ".track", "r")
    route_unprocessed = open(name + ".route", "r")
    os.chdir(os.path.dirname(os.getcwd()))
    os.chdir(os.path.dirname(os.getcwd()))
    outputPath = os.getcwd() + "\\" + "output"
    outputFilePath = outputPath + "\\" + name
    if os.path.exists(outputPath):
        os.chdir(outputPath)
    else:
        os.mkdir(outputPath)
        os.chdir(outputPath)
    if os.path.exists(outputFilePath):
        os.chdir(outputFilePath)
    else:
        os.mkdir(outputFilePath)
        os.chdir(outputFilePath)
    geo = open(name + ".geo", "w")
    dyna = open(name + ".dyna", "w")
    rel = open(name + ".rel", "w")
    usr = open(name + ".usr", "w")
    route_processed = open(name + ".route", "w")


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
    os.chdir(os.path.dirname(os.getcwd()))
    os.chdir(os.path.dirname(os.getcwd()))


def dataTransform(name):
    openFile(name)
    nodeNum = processGeo()
    processUsr()
    processRel()
    processDyna(nodeNum)
    processRoute()
    processConfig()
    closeFile()


if __name__ == '__main__':
    for i in range(100):
        if i < 10:
            name = "0000000" + str(i)
        else:
            name = "000000" + str(i)
        dataTransform(name)
