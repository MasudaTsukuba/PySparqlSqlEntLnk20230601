import json
import os
import pandas as pd
import re
import csv
import sqlite3
import spacy
import csv
from spacy.pipeline import EntityLinker
from src.DatabaseClass import DataBase
from src.PathClass import PathClass


class Uri:
    def __init__(self, path, uri_name):
        self.path = path
        self.uri_path = self.path.dataset_path + '/' + uri_name+'/'  # ./data_set2/uri
        self.uri_dict = {}  # str->uri dictionary
        self.inv_dict = {}  # uri->str dictionary
        self.uri_dict_all = {}
        self.inv_dict_all = {}
        # for file in os.listdir(self.uri_path):  # read PREFIX*.csv
        #     if file.endswith(".csv"):
        #         df = pd.read_csv(self.uri_path + file, header=None)
        #         key = file.replace('.csv', '')  # key = PREFIX_Build, etc.
        #         self.uri_dict[key] = dict(zip(df[0], df[1]))  # str->uri dictionary
        #         self.inv_dict[key] = dict(zip(df[1], df[0]))  # uri->str dictionary
        #         self.uri_dict_all.update(zip(df[0], df[1]))  # all the files in one dictionary
        #         self.inv_dict_all.update(zip(df[1], df[0]))

        # def open_mapping(): not used. replaced by uri_dict, etc.
        #     uri_mapping = './data_set2/uri/URI_mapping.json'
        #     json_open = open(uri_mapping, 'r')
        #     uri_mapping_dict = json.load(json_open)
        #     json_open.close()
        #     return uri_mapping_dict
        # self.uri_mapping_dict = open_mapping()
        # self.entity_linking_file = None
        self.entity_linking_file = self.uri_path+'entity_linking.db'
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
            sql_value = value
            try:
                sql_value = self.inv_dict_all[value]
            except KeyError:
                pass
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
                    # sql_value = self.inv_dict[uri_function][value]
                    sql_value = self.inv_dict_all[value]  # 2023/6/5
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

    # create tables in entity_linking.db database
    def create_entity_linking_db(self):
        conn = sqlite3.connect(self.entity_linking_file)
        cursor = conn.cursor()

        tables = ['country', 'hotel', 'building', 'museum', 'heritage']
        sqls = [
            'CREATE TABLE hotel (id VARCHAR(255) PRIMARY KEY, uri VARCHAR(255), status VARCHAR(255));',
            'CREATE TABLE building (id VARCHAR(255) PRIMARY KEY, uri VARCHAR(255), status VARCHAR(255));',
            'CREATE TABLE museum (id VARCHAR(255) PRIMARY KEY, uri VARCHAR(255), status VARCHAR(255));',
            'CREATE TABLE heritage (id VARCHAR(255) PRIMARY KEY, uri VARCHAR(255), status VARCHAR(255));',
            'CREATE TABLE country (id VARCHAR(255) PRIMARY KEY, uri VARCHAR(255), status VARCHAR(255));'
        ]

        for table in tables:
            sql = f'DROP TABLE {table};'
            try:
                cursor.execute(sql)
                print('DROP TABLE SUCCEEDED: ' + sql)
                pass
            except:
                print('DROP TABLE FAILED: ' + sql)
                pass

        for sql in sqls:
            try:
                cursor.execute(sql)
                print('CREATE TABLE SUCCEEDED: ' + sql)
                pass
            except:
                print('CREATE TABLE FAILED: ' + sql)
                pass

        cursor.close()
        conn.commit()
        conn.close()
        pass

    # for test entity_linking.db
    def test_entity_linking(self):
        conn = sqlite3.connect(self.entity_linking_file)
        cursor = conn.cursor()
        # sql = 'INSERT INTO hotel (id, uri, status) VALUES ("aaa", "bbb", "ccc");'
        # cursor.execute(sql)

        sql = 'SELECT * FROM hotel;'
        return_list = cursor.execute(sql).fetchall()
        headers = [col[0] for col in cursor.description]
        results = [list(i) for i in return_list]
        # print(results)
        cursor.close()
        conn.close()

    # build entity linking
    def build_entity_linking(self):
        conn = sqlite3.connect(self.entity_linking_file)
        cursor = conn.cursor()

        nlp = spacy.load('en_core_web_md')
        # nlp = spacy.load('en_core_web_lg')
        nlp.add_pipe('entityLinker', last=True)

        def spacy_entity_linking(text):
            if text == 'United States of America':  # debug
                pass
            entity = nlp(text)
            result_uri = ''
            result_label = ''
            result_description = ''
            result_status = 'NoFound'
            xxx = entity._.linkedEntities
            my_ents = entity.ents
            found = False
            try:
                yyy = xxx[0]
                result_uri = yyy.get_url()
                result_label = yyy.label
                result_description = yyy.description
                found = True
            except TypeError:
                pass
            except IndexError:
                pass
            mark = '  '
            try:
                if len(xxx) == 1 and result_label == text and found:
                    result_status = 'Succeeded'
                    pass
                else:
                    result_uri = ''
            except IndexError:
                result_uri = ''
                pass
            pass
            return result_status, result_uri, result_label, result_description, xxx

        path = PathClass('')
        database = DataBase(path, 'data_set2', 'landmark.db')
        tables = ['country', 'hotel', 'building', 'museum', 'heritage']
        # tables = ['hotel']  # debug
        conn.execute("BEGIN")
        for table in tables:
            print(table)  # debug
            sql = f'SELECT * FROM {table};'
            results, headers = database.execute(sql)
            for result in results:
                table_id = result[0]
                name = result[1]
                status, uri, label, description, xxx = spacy_entity_linking(name)
                if status == 'Succeeded':
                    uri = uri.replace('https://www.wikidata.org/wiki/', 'http://www.wikidata.org/entity/')
                else:
                    uri = f'http://example.com/id/{table_id}'
                sql = f'INSERT INTO {table} (id, uri, status) VALUES ("{table_id}", "{uri}", "{status}");'
                if status == 'Succeeded' and table_id.replace('h', '') != uri.split('/')[-1].replace('Q', ''):
                    # print(sql)  # debug
                    pass
                cursor.execute(sql)
                pass
            conn.commit()
            query = f'SELECT * FROM {table} WHERE status = "Succeeded" ;'
            results = cursor.execute(query).fetchall()
            for row in results:
                if row[0].replace('h', '') != row[1].split('/')[-1]:
                    pass
            with open(f'uri_{table}.csv', 'w') as f:  # debug
                writer = csv.writer(f)
                writer.writerows(results)
            pass
        cursor.close()
        conn.close()
        pass

    def read_entity_linking(self):
        conn = sqlite3.connect(self.entity_linking_file)
        cursor = conn.cursor()

        tables = ['country', 'hotel', 'building', 'museum', 'heritage']
        for table in tables:  # read from tables in entity_linking.db
            sql = f'SELECT id, uri, status from {table};'
            results = cursor.execute(sql).fetchall()
            for row in results:
                if True:  # row[2] == 'Succeeded':
                    record_id = row[0]
                    record_uri = row[1]
                    self.uri_dict_all[record_id] = record_uri  # all the files in one dictionary
                    self.inv_dict_all[record_uri] = record_id
                    # xxx = self.uri_dict_all['Q30']  # debug
        cursor.close()
        conn.close()
        pass
