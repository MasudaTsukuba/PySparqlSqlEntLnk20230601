import json
import re
import sqlite3
from UriClass import Uri


class SparqlQuery:
    def __init__(self, query_uri, uri_path):
        # ------ ユーザから得て, JSON形式に変換したSPARQLを取り込む --------
        self.var_list = None
        self.filter_list = None
        self.trans_uri_list = None
        self.sql_query = None
        self.exe_query = None
        self.uri = Uri(uri_path)
        json_open = open(query_uri, 'r')
        self.query_dict = json.load(json_open)
        json_open.close()

        def open_mapping():
            uri_mapping = './data_set2/URI/URI_mapping.json'
            json_open = open(uri_mapping, 'r')
            u_mapping_dict = json.load(json_open)
            json_open.close()
            return u_mapping_dict
        self.u_mapping_dict = open_mapping()

        # ------------------------------------------------------------

    def convert_to_sql(self, mapping_class):  # sparql to intermediate sql
        def create_var_list(query_dict):
            # 出力する変数リストを作成
            var_list = []
            for var in query_dict['variables']:  # extract variables from json
                var_list.append(var['value'])  # append to a variable list
            return var_list
        self.var_list = create_var_list(self.query_dict)

        def create_filter_list(query_dict):
            # FILTERの条件リストを作成
            filter_list = []
            for element in query_dict['where']:
                if element['type'] == 'filter':
                    prefix = element['expression']['args']
                    filter_list.append(
                        [prefix[0]['value'],
                         prefix[0]['value'] + ' '  # cname
                         + element['expression']['operator'] + ' "'  # =
                         + prefix[1]['value'] + '"'])  # 'United Kingdom'
            return filter_list
        self.filter_list = create_filter_list(self.query_dict)

        def create_sql_query(query_dict, filter_list, uri):
            # SPARQLクエリの各トリプルパターンから候補のSQLクエリを検索
            sql_query = []
            trans_uri_list = []
            check = []
            checked = []
            for triple in query_dict['where'][0]['triples']:
                """
                {'subject': {'termType': 'Variable', 'value': 's'}, 
                 'predicate': {
                    'termType': 'NamedNode', 
                    'value': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'}, 
                 'object': {
                    'termType': 'NamedNode', 
                    'value': 'http://example.com/ontology/Museum'}}
                """
                sql_subquery = []
                q_predicate = triple['predicate']['value']  # 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
                q_type = triple['predicate']['termType']  # 'NamedNode'
                if q_type != 'Variable':
                    # for j in range(len(mapping_dict)):
                    for mapping in mapping_class.mapping_dict:
                        # predicate = mapping_dict[j]["predicate"]
                        predicate = mapping["predicate"]
                        if q_predicate == predicate:
                            # sql = mapping_dict[j]["SQL"]
                            sql = mapping["SQL"]
                            query = triple
                            # mapping = mapping_dict[j]
                            answer = uri.translate_sql(sql, query, mapping, filter_list)
                            re_sql = answer[0]
                            if answer[1]:
                                for ans in answer[1]:
                                    if ans[0] not in checked:
                                        trans_uri_list.append(ans)
                                        check.append(ans[0])
                            if re_sql != 'No':
                                sql_subquery.append(re_sql)
                    insert_sql = ''
                    if len(sql_subquery) != 0:
                        for sub_q in sql_subquery:
                            insert_sql += sub_q + ' UNION '
                        insert_sql = re.sub(r' UNION $', '', insert_sql)
                        insert_sql = insert_sql.replace(';', '') + ';'
                    checked = checked + check
                    sql_query.append(insert_sql)
                elif q_type == 'Variable':
                    for mapping in mapping_class.mapping_dict:
                        # predicate = mapping_class.mapping_dict[j]["predicate"]
                        sql = mapping["SQL"]
                        query = triple
                        # mapping = mapping_class.mapping_dict[j]
                        answer = uri.translate_sql(sql, query, mapping, filter_list)
                        re_sql = answer[0]
                        if answer[1]:
                            for ans in answer[1]:
                                if ans[0] not in checked:
                                    trans_uri_list.append(ans)
                                    check.append(ans[0])
                        if re_sql != 'No':
                            sql_subquery.append(re_sql)
                    insert_sql = ''
                    if sql_subquery:
                        for sub_q in sql_subquery:
                            insert_sql += sub_q + ' UNION '
                        insert_sql = re.sub(r' UNION $', '', insert_sql)
                        insert_sql = insert_sql.replace(';', '') + ';'
                    checked = checked + check
                    sql_query.append(insert_sql)
            trans_uri_list = list(set(tuple(i) for i in trans_uri_list))
            trans_uri_list = [list(i) for i in trans_uri_list]
            return sql_query, trans_uri_list
        self.sql_query, self.trans_uri_list = create_sql_query(self.query_dict, self.filter_list, self.uri)

        def create_sql(var_list, sql_query):
            select_var = ''
            for var in var_list:  # list of variables in sparql query
                select_var += var + ', '  # change them to a comma-separated string
            select_var = re.sub(', $', '', select_var)  # remove the comma at the end
            exe_query = 'SELECT ' + select_var + ' FROM '
            for item in sql_query:
                exe_query += ' (' + item + ') NATURAL JOIN '  # combine with NATURAL JOIN
            exe_query = re.sub('NATURAL JOIN $', '', exe_query)  # remove the "NATURAL JOIN" at the end
            exe_query = exe_query.replace(';', '') + ';'  # end up with a semicolon while suppressing a duplicate
            # print(exe_query)
            return exe_query  # return the built sql query
        self.exe_query = create_sql(self.var_list, self.sql_query)
        return self.exe_query

    def convert_to_rdf(self, uri_database, results):
        # --------- SQLクエリ結果をSPARQLクエリ結果に合わせるため、必要に応じて文字列->URIに変換する ----------------------------------
        def create_trans_uri_list(trans_uri_list):
            tmp = []
            for i in trans_uri_list:
                if i[1] != 'plain':
                    tmp.append(i)
            return tmp
        trans_uri_list = create_trans_uri_list(self.trans_uri_list)

        def prepare_sql_tquery(u_mapping_dict):
            def g(uri_mapping, b_trans, a_trans):
                sql = str(uri_mapping['SQL'])
                sql = sql.replace(uri_mapping['x'], b_trans)
                sql = sql.replace(uri_mapping['y'], a_trans)
                return sql

            sql_tquery = []
            # print(transURI_list)
            r_list = []
            for s in self.var_list:
                r_list.append(s)
            # print(r_list)
            for y in range(len(r_list)):
                or_query = []
                insert_sql = ''
                i0 = ''
                sql = ''
                for j in trans_uri_list:
                    if j[0] == r_list[y]:
                        for k in u_mapping_dict:
                            if k['name'] == j[1]:
                                i0 = r_list[y] + 'trans'
                                sql = g(k, r_list[y], i0)
                        or_query.append(sql)
                if len(or_query) != 0:
                    for or_q in or_query:
                        insert_sql += or_q + ' UNION '
                    insert_sql = re.sub('UNION $', '', insert_sql)
                    insert_sql = insert_sql.replace(';', '') + ';'

                if insert_sql != '':
                    sql_tquery.append(insert_sql)
                    r_list[y] = i0
            return sql_tquery, r_list
        sql_tquery, r_list = prepare_sql_tquery(self.u_mapping_dict)

        def uri_db(uri_database, var_list):
            c = sqlite3.connect(uri_database)
            c.execute('DROP TABLE Result;')  # ####################2023/3/20
            select_var = ''
            for i in range(len(var_list)):
                if i != len(var_list) - 1:
                    select_var = select_var + var_list[i] + ', '
                else:
                    select_var = select_var + var_list[i]
            c.execute('CREATE TABLE Result(' + select_var + ')')
            v = ''
            for b in range(len(var_list) - 1):
                v = v + '?,'
                if b == len(var_list) - 2:
                    v = v + '?'
            c.executemany('INSERT INTO Result (' + select_var + ') values (' + v + ')', results)
            cur = c.cursor()
            return cur
        cu = uri_db(uri_database, self.var_list)

        # test_query = 'SELECT * FROM Result;'  # debug 20230323
        # test_results = cursor.execute(test_query).fetchall()  # debug 20230323

        def build_query(cursor, r_list, sql_tquery):
            select_var2 = ''
            for r_l in r_list:
                select_var2 += r_l + ', '
            select_var2 = re.sub(', $', '', select_var2)

            # exe_query = 'SELECT ' + select_var2 + ' FROM (Result) '
            exe_query = 'SELECT DISTINCT ' + select_var2 + ' FROM (Result) '  # 20230323
            for sql_tq in sql_tquery:
                # exe_query = exe_query + ' NATURAL JOIN (' + SQL_tquery[i] + ')'
                exe_query = exe_query + ' NATURAL LEFT JOIN (' + sql_tq + ')'  # 20230323
            exe_query = exe_query.replace(';', '') + ';'
            print(exe_query)
            sql_results = cursor.execute(exe_query).fetchall()
            return sql_results
        sparql_results = build_query(cu, r_list, sql_tquery)

        # test_query = 'SELECT s, name, cname FROM Result;'
        # test_results = cursor.execute(test_query).fetchall()  # debug 20230323
        # test_query = 'SELECT * FROM PREFIX_hotel WHERE ID = "15923583h";'
        # test_results = cursor.execute(test_query).fetchall()  # debug 20230323
        # test_query = 'SELECT s, strans, name, cname FROM (Result)  NATURAL LEFT JOIN (SELECT ID AS s, URI_hotel AS strans FROM PREFIX_hotel);'
        # test_results = cursor.execute(test_query).fetchall()  # debug 20230323
        # test_query = 'SELECT DISTINCT s, strans, name, cname FROM (Result)  NATURAL LEFT JOIN (SELECT ID AS s, URI_hotel AS strans FROM PREFIX_hotel);'
        # test_results = cursor.execute(test_query).fetchall()  # debug 20230323
        sparql_results = [list(pp) for pp in sparql_results]  # convert to a list
        return sparql_results

    # def f(self, sql: str, sparql, mapping, filter_list):
    #     # subject
    #
    #     # print(triple)
    #     # print(mapping)
    #
    #     trans_URI = []
    #     if (sparql['subject']['termType'] == 'Variable'):
    #         value = sparql['subject']['value']
    #         sql = sql.replace(mapping['subject'], value)
    #         trans_URI.append([value, mapping['subject_uri']])
    #
    #     elif (sparql['subject']['termType'] == 'NamedNode'):
    #         value = sparql['subject']['value']
    #         uri_function = mapping['subject_uri']
    #         with open(URI_directory + uri_function + '.csv') as g:
    #             reader = csv.reader(g)
    #             for row in reader:
    #                 if (value == row[1]):
    #                     sql_value = row[0]
    #                     break
    #             sql = rewriter.rewrite_where_sql(sql, sql_value, mapping['subject'])
    #
    #     # object
    #     if (sparql['object']['termType'] == 'Variable'):
    #         value = sparql['object']['value']
    #         sql = sql.replace(mapping['object'], value)
    #         trans_URI.append([value, mapping['object_uri']])
    #
    #         for filter in filter_list:
    #             if (filter[0] == value):
    #                 sql = rewriter.rewrite_where_sql_filter(sql, filter[1])
    #
    #
    #     elif (sparql['object']['termType'] == 'NamedNode'):
    #         value = sparql['object']['value']
    #         if (mapping['object_uri'] == '-'):
    #             if (value != mapping['object']):
    #                 return ['No', []]
    #
    #         else:
    #             value = sparql['object']['value']
    #             uri_function = mapping['object_uri']
    #             with open(URI_directory + uri_function + '.csv') as g:
    #                 reader = csv.reader(g)
    #                 for row in reader:
    #                     if (value == row[1]):
    #                         sql_value = '"' + row[0] + '"'
    #                         break
    #             sql = rewriter.rewrite_where_sql(sql, sql_value, mapping['object'])
    #             # sql = sql.replace(mapping['object'], value)
    #
    #     else:  # termTypeが'Literalのとき'
    #         value = sparql['object']['value']
    #         uri_function = mapping['object_uri']
    #         if uri_function == 'plain':
    #             sql = rewriter.rewrite_where_sql(sql, value, mapping['object'])
    #             sql = sql.replace(mapping['object'], value)
    #
    #     return [sql, trans_URI]

