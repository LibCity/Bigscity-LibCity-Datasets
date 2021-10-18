# link: Please contact the author!
import json
import util


def check_all_values(json_obj, feature):
    a = dict()
    for line in json_obj:
        if str(line['properties'][feature]) in a.keys():
            a[str(line['properties'][feature])] += 1
        else:
            a[str(line['properties'][feature])] = 1

    print(feature + ':' + str(a))


def check_fid(json_obj):
    a = set()
    b = list()
    for line in json_obj:
        a.add(line['properties']['fid'])
        b.append(line['properties']['fid'])

    assert len(a) == len(b)


def check_uv(json_obj):
    for line in json_obj:
        if line['properties']['u'] == line['properties']['v']:
            assert True
            # print(line['properties']['u'])


def find_useful_features(feature_list, json_obj):
    res = []
    for feature in feature_list:
        flag = json_obj[0]["properties"][feature]
        for line in json_obj:
            if flag != line["properties"][feature]:
                print(line["properties"][feature])
                res.append(feature)
                break
    return res


def processConfig():
    config = dict()
    config['geo'] = dict()
    config['geo']['including_types'] = ['LineString']
    config['geo']['LineString'] = {}
    config['geo']['LineString']['highway'] = 'num'
    config['geo']['LineString']['length'] = 'num'
    config['geo']['LineString']['lanes'] = 'num'
    config['geo']['LineString']['tunnel'] = 'num'
    config['geo']['LineString']['bridge'] = 'num'
    config['geo']['LineString']['maxspeed'] = 'num'
    config['geo']['LineString']['width'] = 'num'
    config['geo']['LineString']['alley'] = 'num'
    config['geo']['LineString']['roundabout'] = 'num'
    config['rel'] = dict()
    config['rel']['including_types'] = ['geo']
    config['rel']['geo'] = {}
    config['info'] = dict()
    config['info']['geo_file'] = 'bj_roadmap_edge'
    config['info']['rel_file'] = 'bj_roadmap_edge'
    json.dump(config, open('./output/bj_roadmap_edge/config.json', 'w', encoding='utf-8'), ensure_ascii=False)


def get_highway_to_num(json_obj, feature='highway'):
    highway_list = []
    i = 0
    for line in json_obj:
        if isinstance(line['properties'][feature], list):
            highway = line['properties'][feature][0]
        else:
            highway = line['properties'][feature]
        if highway == "unclassified":
            continue
        if str(highway) not in highway_list:
            if str(highway) != "unclassified":
                highway_list.append(str(highway))
    highway_list.sort()
    highway_to_int = {}
    for i in range(0, len(highway_list)):
        highway_to_int[highway_list[i]] = i + 1
    return highway_to_int


def main(file_name):
    file = json.load(open('./input/bj_roadmap_edge/edges.json', 'r', encoding='utf-8'))
    features = file['features']

    # check
    # check_fid(features)  # fid 可以直接作为geo_id
    # check_uv(features)  # 并没有什么毛病...
    check_all_values(features, "lanes")
    highway2num = get_highway_to_num(features, "highway")

    # 2 files
    geo_file = open('./output/bj_roadmap_edge/' + file_name + '.geo', 'w')
    rel_file = open('./output/bj_roadmap_edge/' + file_name + '.rel', 'w')

    # feature_list
    feature_list = ["highway", "length", "lanes", "tunnel", "bridge", "maxspeed", "width", "service", "junction"]
    new_feature_list = find_useful_features(feature_list, features)
    assert feature_list == new_feature_list

    geo_file.write("geo_id,type,coordinates")
    for feature in feature_list:
        if feature == "service":
            feature = "alley"
        if feature == "junction":
            feature = "roundabout"
        geo_file.write(',' + feature)
    geo_file.write('\n')

    for line in features:
        properties = line["properties"]
        geo_id = properties['fid']
        type = 'LineString'
        coordinates = line["geometry"]["coordinates"]
        geo_file.write(str(geo_id) + ',' + type + ',"' + str(coordinates) + '"')
        for feature in feature_list:
            if feature == 'highway':
                if properties[feature] is None or properties[feature] == "unclassified" or properties[feature][0] == "unclassified":
                    geo_file.write(', 0')
                elif isinstance(properties[feature], list):
                    geo_file.write(',' + str(highway2num[str(properties[feature][0])]))
                else:
                    geo_file.write(',' + str(highway2num[str(properties[feature])]))  # no none
            elif feature == 'length':
                geo_file.write(',' + ('0' if properties[feature] is None else str(properties[feature])))
            elif feature == 'lanes':
                if properties[feature] is None:
                    geo_file.write(',' + '0')
                else:
                    geo_file.write(',' + ('0' if properties[feature] is None else str(properties[feature][0])))
            elif feature == 'tunnel':
                geo_file.write(',' + ('0' if properties[feature] is None else '1'))
            elif feature == 'bridge':
                geo_file.write(',' + ('0' if properties[feature] is None else '1'))
            elif feature == 'maxspeed':
                geo_file.write(',' + ('0' if properties[feature] is None else str(properties[feature])))
            elif feature == 'width':
                geo_file.write(',' + ('0' if properties[feature] is None else str(properties[feature])))
            elif feature == 'service':
                geo_file.write(',' + ('0' if properties[feature] is None else str(1)))
            elif feature == 'junction':
                geo_file.write(',' + ('0' if properties[feature] is None else str(1)))

        geo_file.write('\n')

    # 每个点的入边和出边
    in_dict, out_dict = dict(), dict()
    for line in features:
        u, v = line["properties"]['u'], line["properties"]['v']
        fid = line["properties"]['fid']
        if u not in out_dict:
            out_dict[u] = [fid]
        else:
            out_dict[u].append(fid)
        if v not in in_dict:
            in_dict[v] = [fid]
        else:
            in_dict[v].append(fid)

    # rel file
    rel_file.write('rel_id,type,origin_id,destination_id\n')
    i = 0
    for key, in_list in in_dict.items():
        if key in out_dict.keys():
            out_list = out_dict[key]
            for a in in_list:
                for b in out_list:
                    rel_file.write(str(i) + ',geo,' + str(a) + ',' + str(b) + '\n')
                    i += 1
    json.dump(highway2num, open('./output/bj_roadmap_edge/highway2num.json', 'w'))

    processConfig()


if __name__ == '__main__':
    outputdir = './output/bj_roadmap_edge'
    util.ensure_dir(outputdir)
    main('bj_roadmap_edge')
    # import pandas as pd
    # a = pd.read_csv('./output/bj_roadmap_edge/bj_roadmap_edge.rel')
    # print(a)
