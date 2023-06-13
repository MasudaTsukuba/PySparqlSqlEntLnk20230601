# class for handling a mapping rules
# 2023/6/1, Tadashi masuda

import json


class Mapping:
    def __init__(self, path):
        mapping_uri = path  # './data_set2/mapping/mapping.json'
        # マッピングデータの取り込み
        json_open_file = open(mapping_uri, 'r')
        self.mapping_dict = json.load(json_open_file)
        json_open_file.close()
