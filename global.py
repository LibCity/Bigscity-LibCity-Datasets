#link: https://zenodo.org/record/57731#.YSJRbGczZPZ
import os
import shutil

def processGeo(name):
    geo = open(name + ".geo", "w")
    geo.write("geo_id, type, coordinate\n")
    nodes = open(name + ".nodes", "r")
    i = 0
    node = nodes.readline()[0 : -1]
    while node != "":
        node1 = node.split("\t")[0]
        node2 = node.split("\t")[1]
        geo.write(str(i) + ',Point,"[' + node1 + ',' + node2 + ']"\n')
        i += 1
        node = nodes.readline()[0 : -1]
    nodes.close()
    nodeNum = i

    nodes = open(name + ".track", "r")
    node = nodes.readline()[0 : -1]
    while node != "":
        node1 = node.split("\t")[0]
        node2 = node.split("\t")[1]
        geo.write(str(i) + ',Point,"[' + node1 + ',' + node2 + ']"\n')
        i += 1
        node = nodes.readline()[0 : -1]
    nodes.close()
    geo.close()
    return nodeNum

def processUsr(name):
    usr = open(name + ".usr", "w")
    usr.write("usr_id\n")
    usr.write("0")
    usr.close()

def processRel(name):
    rel = open(name + ".rel", "w")
    rel.write("rel_id,type,origin_id,destination_id\n")
    arcs = open(name + ".arcs", "r")
    i = 0
    arc = arcs.readline()[0 : -1]
    while arc != "":
        arc = arc.replace('\t', ',')
        rel.write(str(i) + ',geo,' + arc + '\n')
        i += 1
        arc = arcs.readline()[0: -1]
    arcs.close()
    rel.close()

def processDyna(name, nodeNum):
    dyna = open(name + ".dyna", "w")
    dyna.write("dyna_id,type,time,entity_id,location\n")
    nodes = open(name + ".track", "r")
    i = 0
    node = nodes.readline()[0: -1]
    while node != "":
        dyna.write(str(i) + ',trajectory,' + ',0,' + str(i + nodeNum) + '\n')
        i += 1
        node = nodes.readline()[0: -1]
    nodes.close()
    dyna.close()

def processConfig(name):
    # copy config.json file in 00000000
    if name != "00000000":
        shutil.copyfile("../00000000/config.json", "config.json")

def delete(name):
    path = os.getcwd() + "\\" + name + ".arcs"
    if os.path.exists(path):
        os.remove(path)
    path = os.getcwd() + "\\" + name + ".nodes"
    if os.path.exists(path):
        os.remove(path)
    path = os.getcwd() + "\\" + name + ".route"
    if os.path.exists(path):
        os.remove(path)
    path = os.getcwd() + "\\" + name + ".track"
    if os.path.exists(path):
        os.remove(path)

def dataTransform(name):
    processConfig(name)
    nodeNum = processGeo(name)
    processUsr(name)
    processRel(name)
    processDyna(name, nodeNum)
    delete(name)

if __name__ == '__main__':
    for i in range(1):
        if i < 10:
            name = "0000000" + str(i)
        else:
            name = "000000" + str(i)
        file_dir = os.chdir(name)
        dataTransform(name)
        os.chdir(os.path.dirname(os.getcwd()))


