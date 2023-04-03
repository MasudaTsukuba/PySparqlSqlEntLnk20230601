import json


class Mapping:
    def __init__(self):
        mapping_uri = './data_set2/mapping/mapping.json'
        # マッピングデータの取り込み
        json_open_file = open(mapping_uri, 'r')
        self.mapping_dict = json.load(json_open_file)
        json_open_file.close()
