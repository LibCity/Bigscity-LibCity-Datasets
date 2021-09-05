#link: https://www.microsoft.com/en-us/research/publication/hidden-markov-map-matching-noise-sparseness/
import re
import os
import json

network = trackInfo = truth = geo = rel = dyna = usr = route = None


def processGeoAndRelAndRoute():
    geo.write("geo_id, type, coordinate\n")
    rel.write("rel_id,type,origin_id,destination_id\n")
    network.readline()
    line = network.readline()
    nodeInfo = re.search("\(.+\)", line)
    j = 0
    currentSum = 0
    dic = {}
    while nodeInfo is not None:
        flag = line.split('\t')[3]
        nodes = nodeInfo[0].replace('(', '').replace(')', '').split(', ')
        i = 0
        while i < len(nodes):
            node1 = nodes[i].split(" ")[0]
            node2 = nodes[i].split(" ")[1]
            geo.write(str(j) + ',Point,"[' + node1 + ',' + node2 + ']"\n')
            if i != len(nodes) - 1:
                rel.write(str(currentSum) + ',geo,' + str(j) + ',' + str(j + 1) + '\n')
                if line.split('\t')[0] in dic.keys():
                    dic[line.split('\t')[0]].append(currentSum)
                else:
                    dic[line.split('\t')[0]] = [currentSum]
                currentSum += 1
                if flag == '1':
                    rel.write(str(currentSum) + ',geo,' + str(j + 1) + ',' + str(j) + '\n')
                    currentSum += 1
            i += 1
            j += 1
        line = network.readline()
        nodeInfo = re.search("\(.+\)", line)
    nodeNum = j

    trackInfo.readline()
    nodeInfo = re.split('\t| ', trackInfo.readline())
    while len(nodeInfo) == 6:
        node1 = nodeInfo[3]
        node2 = nodeInfo[2]
        geo.write(str(j) + ',Point,"[' + node1 + ',' + node2 + ']"\n')
        j += 1
        nodeInfo = re.split('\t| ', trackInfo.readline())

    route.write("rel_id\n")
    truth.readline()
    truth_info = truth.readline()
    while truth_info != '':
        edge_id = truth_info.split("\t")[0]
        traversed = truth_info.split("\t")[1].replace('\n', '')
        if traversed == '1':
            i = 0
            while i < len(dic[edge_id]):
                route.write(str(dic[edge_id][i]) + '\n')
                i += 1
        else:
            i = len(dic[edge_id]) - 1
            while i >= 0:
                route.write(str(dic[edge_id][i]) + '\n')
                i -= 1
        truth_info = truth.readline()
    return nodeNum


def processUsr():
    usr.write("usr_id\n")
    usr.write("0")


def processDyna(nodeNum):
    dyna.write("dyna_id,type,time,entity_id,location\n")
    trackInfo.seek(0)
    trackInfo.readline()
    nodeInfo = re.split('\t| ', trackInfo.readline())
    i = 0
    while len(nodeInfo) == 6:
        second = nodeInfo[1]
        time = "2009-01-17T" + second + 'Z'
        dyna.write(str(i) + ',trajectory,' + time + ',0,' + str(i + nodeNum) + '\n')
        i += 1
        nodeInfo = re.split('\t| ', trackInfo.readline())


def processConfig():
    config = dict()
    config['geo'] = dict()
    config['geo']['including_types'] = ['Point']
    config['geo']['Point'] = {}
    config['usr'] = dict()
    config['usr']['properties'] = {}
    config['rel'] = dict()
    config['rel']['including_types'] = ['geo']
    config['rel']['geo'] = {'speed': 'num'}
    config['dyna'] = dict()
    config['dyna']['including_types'] = ['trajectory']
    config['dyna']['trajectory'] = {'entity_id': 'usr_id', 'location': 'geo_id'}
    config['info'] = dict()
    json.dump(config, open('config.json', 'w', encoding='utf-8'), ensure_ascii=False)


def openFile():
    global network, trackInfo, truth, geo, rel, dyna, usr, route
    os.chdir("input")
    network = open("road_network.txt", "r")
    trackInfo = open("gps_data.txt", "r")
    truth = open("ground_truth_route.txt", "r")
    os.chdir(os.path.dirname(os.getcwd()))
    outputPath = os.getcwd() + "\\" + "output"
    if os.path.exists(outputPath):
        os.chdir(outputPath)
    else:
        os.mkdir(outputPath)
    geo = open("Seattle.geo", "w")
    rel = open("Seattle.rel", "w")
    dyna = open("Seattle.dyna", "w")
    usr = open("Seattle.usr", "w")
    route = open("Seattle.route", "w")


def closeFile():
    global network, trackInfo, truth, geo, rel, dyna, usr, route
    network.close()
    trackInfo.close()
    truth.close()
    geo.close()
    rel.close()
    dyna.close()
    usr.close()
    route.close()


def dataTransform():
    openFile()
    nodeNum = processGeoAndRelAndRoute()
    processUsr()
    processDyna(nodeNum)
    processConfig()
    closeFile()


if __name__ == '__main__':
    dataTransform()
