from util import ensure_dir
import json


output_dir = "output/bj_roadmap_node"
ensure_dir(output_dir)
nodes = json.load(open('input/bj_roadmap_node/nodes.json', 'r', encoding='utf-8'))
edges = json.load(open('input/bj_roadmap_node/edges.json', 'r', encoding='utf-8'))

node_features = nodes['features']
edge_features = edges['features']

nodes = {}

for feature in node_features:
    assert feature['geometry']['type'] == 'Point'
    properties = feature['properties']
    osmid = properties['osmid']

    assert osmid not in nodes
    nodes[osmid] = properties




