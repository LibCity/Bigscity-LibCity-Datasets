import json


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


def get_highway_to_num(json_obj, feature='highway'):
    res = dict()
    i = 0
    for line in json_obj:
        if str(line['properties'][feature]) not in res.keys():
            res[str(line['properties'][feature])] = i
            i += 1
    return res


def main(file_name):
    file = json.load(open('./data/edges.json', 'r', encoding='utf-8'))
    features = file['features']

    # check
    # check_fid(features)  # fid 可以直接作为geo_id
    # check_uv(features)  # 并没有什么毛病...
    check_all_values(features, "lanes")
    highway2num = get_highway_to_num(features, "highway")

    # 2 files
    geo_file = open('./data/' + file_name + '.geo', 'w')
    rel_file = open('./data/' + file_name + '.rel', 'w')

    # feature_list
    feature_list = ["highway", "length", "lanes", "tunnel", "bridge", "maxspeed", "width", "service", "junction", "key"]
    new_feature_list = find_useful_features(feature_list, features)
    assert feature_list == new_feature_list

    geo_file.write("geo_id,type,coordinates")
    for feature in feature_list:
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
            elif feature == 'key':
                geo_file.write(',' + str(properties[feature]))

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
    json.dump(highway2num, open('./data/highway2num.json', 'w'))


if __name__ == '__main__':
    main('BJ_roadmap')
    # import pandas as pd
    # a = pd.read_csv('./data/BJ_roadmap.rel')
    # print(a)
