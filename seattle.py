#link: https://www.microsoft.com/en-us/research/publication/hidden-markov-map-matching-noise-sparseness/
import re

def processGeoAndRel():
    geo = open("Seattle.geo", "w")
    geo.write("geo_id, type, coordinate, speed\n")
    rel = open("Seattle.rel", "w")
    rel.write("rel_id,type,origin_id,destination_id\n")
    network = open("road_network.txt", "r")
    network.readline()
    line = network.readline()
    nodeInfo = re.search("\(.+\)", line)
    j = 0
    currentSum = 0
    while nodeInfo != None:
        flag = line.split('\t')[3]
        speed = line.split('\t')[4]
        nodes = nodeInfo[0].replace('(', '').replace(')', '').split(', ')
        i = 0
        while i < len(nodes):
            node1 = nodes[i].split(" ")[0]
            node2 = nodes[i].split(" ")[1]
            geo.write(str(j) + ',Point,"[' + node1 + ',' + node2 + ']"\n')
            if i != len(nodes) - 1:
                rel.write(str(currentSum) + ',geo,' + str(j) + ',' + str(j + 1) + ',' + speed + '\n')
                currentSum += 1
                if flag == '1':
                    rel.write(str(currentSum) + ',geo,' + str(j + 1) + ',' + str(j) + ',' + speed + '\n')
                    currentSum += 1
            i += 1
            j += 1
        line = network.readline()
        nodeInfo = re.search("\(.+\)", line)
    network.close()
    rel.close()
    nodeNum = j

    trackInfo = open("gps_data.txt", "r")
    trackInfo.readline()
    nodeInfo = re.split('\t| ', trackInfo.readline())
    while len(nodeInfo) == 6:
        node1 = nodeInfo[3]
        node2 = nodeInfo[2]
        geo.write(str(j) + ',Point,"[' + node1 + ',' + node2 + ']"\n')
        j += 1
        nodeInfo = re.split('\t| ', trackInfo.readline())
    trackInfo.close()
    geo.close()
    return nodeNum

def processUsr():
    usr = open("Seattle.usr", "w")
    usr.write("usr_id\n")
    usr.write("0")
    usr.close()

def processDyna(nodeNum):
    dyna = open("Seattle.dyna", "w")
    dyna.write("dyna_id,type,time,entity_id,location\n")
    trackInfo = open("gps_data.txt", "r")
    trackInfo.readline()
    nodeInfo = re.split('\t| ', trackInfo.readline())
    i = 0
    while len(nodeInfo) == 6:
        second = nodeInfo[1]
        time = "2009-01-17T" + second + 'Z'
        dyna.write(str(i) + ',trajectory,' + time + ',0,' + str(i + nodeNum) + '\n')
        i += 1
        nodeInfo = re.split('\t| ', trackInfo.readline())
    trackInfo.close()
    dyna.close()

def dataTransform():
    nodeNum = processGeoAndRel()
    processUsr()
    processDyna(nodeNum)

if __name__ == '__main__':
    dataTransform()

