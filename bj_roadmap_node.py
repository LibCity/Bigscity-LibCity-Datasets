import csv
import json
import os
from util import ensure_dir

DATA_NAME = "bj_roadmap_node"

# dataset preprocess
# get dataset values
# edge_feature_names = ["oneway", "highway", "lanes", "tunnel", "bridge", "access", "service", "junction", "key"]
# edge_feature_sets = {}
# for edge_feature in edge_feature_names:
#     edge_feature_sets[edge_feature] = set()
# for all_feature in edge_features:
#     feature = all_feature['properties']
#     for edge_feature in edge_feature_names:
#         if feature[edge_feature] is not None:
#             if isinstance(feature[edge_feature], list):
#                 print(feature[edge_feature])
#                 for subfeature in feature[edge_feature]:
#                     x = subfeature
#                     edge_feature_sets[edge_feature].add(x)
#             else:
#                 x = feature[edge_feature]
#                 edge_feature_sets[edge_feature].add(x)
#
# print(edge_feature_sets)

# node feature
node_highways = [None, 'traffic_signals', 'motorway_junction', 'bus_stop', 'crossing', 'turning_circle']
node_highway_to_int = {}
for i in range(0, len(node_highways)):
    node_highway_to_int[node_highways[i]] = i

# edge feature
edge_feature_values = {'oneway': {False, 'True', 'False', True},
                       'highway': {'trunk', 'road', 'motorway', 'residential', 'trunk_link', 'tertiary', 'unclassified',
                                   'primary_link', 'secondary', 'tertiary_link', 'secondary_link', 'motorway_link',
                                   'living_street', 'primary'},
                       'lanes': {'4', '3', '2', '1', '5'},  # int()
                       'tunnel': {'yes', 'building_passage'},  # building_passage = yes
                       'bridge': {'viaduct', 'yes'},  # viaduct = yes
                       'access': {'designated', 'destination', 'yes', 'no', 'permissive', 'unknown'},  # useless
                       'service': {'alley'},  # if alley
                       'junction': {'roundabout'}}  # if roundabout
edge_feature_values['highway'].remove("unclassified")  # useless
highway_list = list(edge_feature_values['highway'])
highway_list.sort()
highway_to_int = {}
for i in range(0, len(highway_list)):
    highway_to_int[highway_list[i]] = i + 1
print(highway_to_int)

rel_feature_names = ["highway", "length", "lanes", "tunnel", "bridge", "maxspeed", "width", "alley", "roundabout"]

# input
nodes = json.load(open(os.path.join(os.path.join('input', DATA_NAME), 'nodes.json'), 'r', encoding='utf-8'))
edges = json.load(open(os.path.join(os.path.join('input', DATA_NAME), 'edges.json'), 'r', encoding='utf-8'))

# output
output_dir = os.path.join("output", DATA_NAME)
ensure_dir(output_dir)

geo_file = open(os.path.join(output_dir, DATA_NAME + ".geo"), "w", newline='')
geo_writer = csv.writer(geo_file)
geo_writer.writerow(["geo_id", "type", "coordinates", "highway"])

rel_file = open(os.path.join(output_dir, DATA_NAME + ".rel"), "w", newline='')
rel_writer = csv.writer(rel_file)
rel_titles = ["rel_id", "type", "origin_id", "destination_id"] + rel_feature_names
rel_writer.writerow(rel_titles)

node_features = nodes['features']
edge_features = edges['features']

nodes = {}
coords_to_osm = {}

for feature in node_features:
    assert feature['geometry']['type'] == 'Point'
    properties = feature['properties']
    osmid = properties['osmid']

    assert osmid not in nodes
    nodes[osmid] = properties
    coords_to_osm[(properties["x"], properties["y"])] = osmid
    geo_writer.writerow([osmid, "trajectory", [properties["x"], properties["y"]],
                         node_highway_to_int[properties["highway"]]])

for feature in edge_features:
    properties = feature["properties"]
    rel_properties = {"highway": 0 if properties["highway"] is None or properties["highway"] == "unclassified"
                                      or properties["highway"][0] == "unclassified"
                                   else highway_to_int[properties["highway"][0]] if isinstance(properties["highway"], list)
                                   else highway_to_int[properties["highway"]],
                      "lanes": 0 if properties["lanes"] is None
                                 else int(properties["lanes"][0]) if isinstance(properties["lanes"], list)
                                 else int(properties["lanes"]),
                      "tunnel": 0 if properties["tunnel"] is None else 1,
                      "bridge": 0 if properties["bridge"] is None else 1,
                      "alley": 0 if properties["service"] is None else 1,
                      "maxspeed": 0 if properties["maxspeed"] is None else float(properties["maxspeed"]),
                      "length": 0 if properties["length"] is None else float(properties["length"]),
                      "width": 0 if properties["width"] is None else float(properties["width"]),
                      "roundabout": 0 if properties["junction"] is None else 1}

    rel_output = [properties["fid"], "geo", int(properties["u"]), int(properties["v"])]
    for feature_name in rel_feature_names:
        rel_output.append(rel_properties[feature_name])
    rel_writer.writerow(rel_output)

    # check how many lon-la coords in nodes.json in LineString between points
    # coords_u = (nodes[properties["u"]]["x"], nodes[properties["u"]]["y"])
    # coords_v = (nodes[properties["v"]]["x"], nodes[properties["v"]]["y"])
    # results: only 5 points
    # geometry = feature["geometry"]
    # assert geometry["type"] == "LineString"
    # for node in geometry['coordinates']:
    #     coords = (node[0], node[1])
    #     if coords_u != coords and coords_v != coords:
    #         if coords in coords_to_osm:
    #             print(node)
    #         continue
    #     osm_id = coords_to_osm[coords]

# check if direct graph, not fused graph
# mp = {}
# for feature in edge_features:
#     properties = feature["properties"]
#     if properties["oneway"] is False or properties["oneway"] == "False":
#         u = properties["u"]
#         v = properties["v"]
#         if (u, v) not in mp:
#             mp[(u, v)] = mp[(v, u)] = 1
#         else:
#             mp[(u, v)] = mp[(v, u)] = 0
#
# for (u, v) in mp:
#     assert mp[(u, v)] == 0

# config
config = dict()
config['geo'] = dict()
config['geo']['including_types'] = ['Point']
config['geo']['Point'] = dict()
config['geo']['Point']['highway'] = 'num'
config['rel'] = dict()
config['rel']['including_types'] = ['geo']
config['rel']['geo'] = {"highway": 'num', "length": 'num', "lanes": 'num',
                        "tunnel": 'num', "bridge": 'num', "maxspeed": 'num',
                        "width": 'num', "alley": 'num', "roundabout": 'num'}
json.dump(config, open(os.path.join(output_dir, 'config.json'),
                       'w', encoding='utf-8'), ensure_ascii=False)

