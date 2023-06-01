# class for handling paths
# 2023/6/1, Tadashi Masuda

import os


class PathClass:
    def __init__(self, input_query_file):
        # root of working path
        self.working_path = os.getcwd()
        if self.working_path.endswith('src'):
            self.working_path = os.path.dirname(self.working_path)

        # path storing queries
        # self.common_query_path = os.path.dirname(self.working_path)+'/PySparqlQuery20230508/'
        # self.common_query_path = os.path.dirname(self.working_path)+'/PySparqlSatoNew20230509/query/'
        self.common_query_path = self.working_path + '/query/'

        # name of input query file
        self.input_query_file = input_query_file

        # path to output file
        output_file = self.input_query_file.replace('.txt', '.csv')
        self.output_file_path = f'{self.working_path}/output/{output_file}'

        # path to mapping file
        self.mapping_file_path = self.working_path + '/data_set2/mapping/mapping_revised.json'
        pass
