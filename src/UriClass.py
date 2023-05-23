import json
import os
import pandas as pd
import re


class Uri:
    def __init__(self, path):
        self.uri_directory = path  # ./data_set2/URI
        self.uri_dict = {}  # str->uri dictionary
        self.inv_dict = {}  # uri->str dictionary
        self.uri_dict_all = {}
        self.inv_dict_all = {}
        for file in os.listdir(path):  # read PREFIX*.csv
            if file.endswith(".csv"):
                df = pd.read_csv(path+file, header=None)
                key = file.replace('.csv', '')  # key = PREFIX_Build, etc.
                self.uri_dict[key] = dict(zip(df[0], df[1]))  # str->uri dictionary
                self.inv_dict[key] = dict(zip(df[1], df[0]))  # uri->str dictionary
                self.uri_dict_all.update(zip(df[0], df[1]))  # all the files in one dictionary
                self.inv_dict_all.update(zip(df[1], df[0]))

        # def open_mapping(): not used. replaced by uri_dict, etc.
        #     uri_mapping = './data_set2/URI/URI_mapping.json'
        #     json_open = open(uri_mapping, 'r')
        #     uri_mapping_dict = json.load(json_open)
        #     json_open.close()
        #     return uri_mapping_dict
        # self.uri_mapping_dict = open_mapping()

        pass

    def translate_sql(self, sql: str, triple, mapping, filter_list):  # uri translation: return [sql, trans_uri]
        def rewrite_where_sql(sql: str, where_value, var):
            if 'WHERE' in sql:
                # print('A')
                index = sql.find(';')
                re_sql = sql[:index] + ' AND ' + var + ' = "' + where_value + '";'
            else:
                index = sql.find(';')
                re_sql = sql[:index] + ' WHERE ' + var + ' = "' + where_value + '";'
            return re_sql

        def rewrite_where_sql_filter(sql: str, sql_filter):
            pattern = r'"(http://.*)"'
            matches = re.findall(pattern, sql_filter)
            if matches:
                for match in matches:
                    try:
                        replacement = self.inv_dict_all[match]
                        sql_filter = re.sub(match, replacement, sql_filter)
                    except KeyError:
                        pass
            if 'WHERE' in sql:
                # print('A')
                index = sql.find(';')
                re_sql = sql[:index] + ' AND ' + sql_filter + ';'
            else:
                index = sql.find(';')
                re_sql = sql[:index] + ' WHERE ' + sql_filter + ';'
            return re_sql

        def create_trans_uri(triple, sql, key):
            value = triple[key]['value']  # get the name of the variable
            sql_replace = sql.replace(mapping[key]['variable'], value)  # replace the variable in sql statement
            try:
                trans_uri.append([value, mapping[key]['uri']])
            except KeyError:
                pass
            return trans_uri, sql_replace, value

        trans_uri = []  # translation uri; will be returned
        # subject
        if triple['subject']['termType'] == 'Variable':  # in the case the subject is a variable
            trans_uri, sql, value = create_trans_uri(triple, sql, 'subject')
        elif triple['subject']['termType'] == 'NamedNode':  # in the case the subject is a constant
            value = triple['subject']['value']
            uri_function = mapping['subject']['uri']
            # with open(self.uri_directory + uri_function + '.csv') as g:
            #     reader = csv.reader(g)
            #     for row in reader:
            #         if value == row[1]:
            #             sql_value = row[0]
            #             break
            # sql_value = self.inv_dict[uri_function][value]
            # sql_value = self.inv_dict[uri_function][value]
            sql_value = self.inv_dict_all[value]
            sql = rewrite_where_sql(sql, sql_value, mapping['subject']['variable'])

        # predicate  # 2023/5/23
        if triple['predicate']['termType'] == 'Variable':
            trans_uri, sql, value = create_trans_uri(triple, sql, 'predicate')

        # object
        if triple['object']['termType'] == 'Variable':
            # value = triple['object']['value']
            # sql = sql.replace(mapping['object'], value)
            # trans_uri.append([value, mapping['object_uri']])
            trans_uri, sql, value = create_trans_uri(triple, sql, 'object')

            for filter_item in filter_list:
                if filter_item[0] == value:
                    sql = rewrite_where_sql_filter(sql, filter_item[1])

        elif triple['object']['termType'] == 'NamedNode':
            value = triple['object']['value']
            if mapping['object']['uri'] == '-':
                if value != mapping['object']['content']:
                    return ['No', []]
            elif mapping['object']['uri'] == 'plain':  # 2023/5/8
                sql_value = value  # 2023/5/8
                sql = rewrite_where_sql(sql, sql_value, mapping['object']['variable'])  # 2023/5/8
            else:
                value = triple['object']['value']
                uri_function = mapping['object']['uri']
                # with open(self.uri_directory + uri_function + '.csv') as g:
                #     reader = csv.reader(g)
                #     for row in reader:
                #         if value == row[1]:
                #             sql_value = '"' + row[0] + '"'
                #             break
                try:  # 2023/5/8
                    sql_value = self.inv_dict[uri_function][value]
                    sql = rewrite_where_sql(sql, sql_value, mapping['object'])
                    # sql = sql.replace(mapping['object'], value)
                except KeyError:  # 2023/5/8
                    return ['No', []]  # 2023/5/8
        else:  # termTypeが'Literalのとき'
            value = triple['object']['value']
            uri_function = mapping['object']['uri']
            if uri_function == 'plain':
                sql = rewrite_where_sql(sql, value, mapping['object'])
                sql = sql.replace(mapping['object'], value)

        return [sql, trans_uri]

    # def g(uri_mapping, b_trans, a_trans):
    #     sql = str(uri_mapping['SQL'])
    #     sql = sql.replace(uri_mapping['x'], b_trans)
    #     sql = sql.replace(uri_mapping['y'], a_trans)
    #     return sql

