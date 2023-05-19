import json
import re
import sqlite3
# from UriClass import Uri


class SparqlQuery:
    def __init__(self, query_uri, uri):
        # ------ ユーザから得て, JSON形式に変換したSPARQLを取り込む --------
        self.var_list = None  # list of variables
        self.filter_list = None  # list of filters
        self.trans_uri_list = None
        self.sql_query = None
        self.exe_query = None
        self.uri = uri  # instance of Uri class
        json_open = open(query_uri, 'r')
        self.query_in_json = json.load(json_open)  # read query in json format
        json_open.close()

        # def open_mapping():
        #     uri_mapping = './data_set2/URI/URI_mapping.json'
        #     json_open = open(uri_mapping, 'r')
        #     uri_mapping_dict = json.load(json_open)
        #     json_open.close()
        #     return uri_mapping_dict
        # self.uri_mapping_dict = open_mapping()

        # ------------------------------------------------------------

    def convert_to_sql(self, mapping_class):  # sparql to intermediate sql
        def create_var_list(query_dict):
            # 出力する変数リストを作成
            var_list = []
            for var in query_dict['variables']:  # extract variables from json
                var_list.append(var['value'])  # append to a variable list
            return var_list
        self.var_list = create_var_list(self.query_in_json)

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
        self.filter_list = create_filter_list(self.query_in_json)

        def create_sql_query(query_dict, filter_list, uri):
            # SPARQLクエリの各トリプルパターンから候補のSQLクエリを検索
            sql_query = []  # return sql
            trans_uri_list = []  # return value
            check = []
            checked = []
            for triple in query_dict['where'][0]['triples']:  # process each triple in sparql query
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
                if q_type == 'NamedNode':  # in case the predicate is not variable
                    # for j in range(len(mapping_dict)):
                    for mapping in mapping_class.mapping_dict:  # search mapping rules
                        # predicate = mapping_dict[j]["predicate"]
                        predicate = mapping["predicate"]["content"]
                        if q_predicate == predicate:  # predicates matched
                            # sql = mapping_dict[j]["SQL"]
                            sql = mapping["SQL"]  # sql statement in the mapping file
                            # query = triple
                            # mapping = mapping_dict[j]
                            answer = uri.translate_sql(sql, triple, mapping, filter_list)
                            re_sql = answer[0]  # uri translated sql
                            if answer[1]:
                                for ans in answer[1]:
                                    if ans[0] not in checked:
                                        trans_uri_list.append(ans)
                                        check.append(ans[0])
                            if re_sql != 'No':
                                sql_subquery.append(re_sql)
                    insert_sql = ''
                    if len(sql_subquery) != 0:  # matches are found
                        for sub_q in sql_subquery:
                            insert_sql += sub_q + ' UNION '  # connect sqls with union
                        insert_sql = re.sub(r' UNION $', '', insert_sql)  # remove the last 'UNION'
                        insert_sql = insert_sql.replace(';', '') + ';'  # add a semicolon at the end
                    checked = checked + check
                    sql_query.append(insert_sql)  # append into a list
                elif q_type == 'Variable':
                    for mapping in mapping_class.mapping_dict:
                        # predicate = mapping_class.mapping_dict[j]["predicate"]
                        sql = mapping["SQL"]
                        # query = triple
                        # mapping = mapping_class.mapping_dict[j]
                        answer = uri.translate_sql(sql, triple, mapping, filter_list)
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
        self.sql_query, self.trans_uri_list = create_sql_query(self.query_in_json, self.filter_list, self.uri)

        def create_sql(var_list, sql_query):
            select_var = ''
            for var in var_list:  # list of variables in sparql query
                select_var += var + ', '  # change them to a comma-separated string
            select_var = re.sub(', $', '', select_var)  # remove the comma at the end
            exe_query = 'SELECT ' + select_var + ' FROM '
            try:
                if self.query_in_json['distinct']:
                    exe_query = 'SELECT DISTINCT ' + select_var + ' FROM '
            except KeyError:
                pass
            for item in sql_query:
                exe_query += ' (' + item + ') NATURAL JOIN '  # combine with NATURAL JOIN
            exe_query = re.sub('NATURAL JOIN $', '', exe_query)  # remove the "NATURAL JOIN" at the end
            exe_query = exe_query.replace(';', '') + ';'  # end up with a semicolon while suppressing a duplicate
            # print(exe_query)
            return exe_query  # return the built sql query
        self.exe_query = create_sql(self.var_list, self.sql_query)
        return self.exe_query

    def convert_to_rdf(self, uri, results):
        # --------- SQLクエリ結果をSPARQLクエリ結果に合わせるため、必要に応じて文字列->URIに変換する ----------------------------------
        # def create_trans_uri_list(trans_uri_list):
        #     tmp = []
        #     for i in trans_uri_list:
        #         if i[1] != 'plain':
        #             tmp.append(i)
        #     return tmp
        # trans_uri_list = create_trans_uri_list(self.trans_uri_list)
        #
        # def prepare_sql_tquery(uri):
        #     def g(uri_mapping, b_trans, a_trans):
        #         sql = str(uri_mapping['SQL'])  # "SQL":"SELECT ID AS A0, URI_Build AS A1 FROM PREFIX_Build"
        #         sql = sql.replace(uri_mapping['x'], b_trans)  # "x":"A0"
        #         sql = sql.replace(uri_mapping['y'], a_trans)  # "y":"A1"
        #         return sql
        #
        #     sql_tquery = []
        #     # print(transURI_list)
        #     r_list = []
        #     for s in self.var_list:
        #         r_list.append(s)
        #     # print(r_list)  # ['s', 'name', 'cname']
        #     for y in range(len(r_list)):
        #         or_query = []
        #         insert_sql = ''
        #         i0 = ''
        #         sql = ''
        #         for j in trans_uri_list:  # [['s', 'PREFIX_museum'],...]
        #             if j[0] == r_list[y]:  # name of a variable
        #                 for k in uri:
        #                     if k['name'] == j[1]:  # mapping table/function
        #                         i0 = r_list[y] + 'trans'  # s -> strans
        #                         sql = g(k, r_list[y], i0)  # query for search PREFIX*
        #                 or_query.append(sql)
        #         if len(or_query) != 0:
        #             for or_q in or_query:
        #                 insert_sql += or_q + ' UNION '  # combine with 'UNION
        #             insert_sql = re.sub('UNION $', '', insert_sql)  # remove the last 'UNION'
        #             insert_sql = insert_sql.replace(';', '') + ';'  # add semicolon at the end while suppressing the duplicate
        #
        #         if insert_sql != '':
        #             sql_tquery.append(insert_sql)  # list of sql's
        #             r_list[y] = i0  # replace the list of variables
        #     return sql_tquery, r_list
        # sql_tquery, r_list = prepare_sql_tquery(self.uri)
        #
        # def uri_db(uri_database, var_list):
        #     c = sqlite3.connect(uri_database)
        #     c.execute('DROP TABLE Result;')  # ####################2023/3/20
        #     select_var = ''
        #     for i in range(len(var_list)):
        #         if i != len(var_list) - 1:
        #             select_var = select_var + var_list[i] + ', '
        #         else:
        #             select_var = select_var + var_list[i]
        #     c.execute('CREATE TABLE Result(' + select_var + ')')  # empty table
        #     v = ''
        #     # for b in range(len(var_list) - 1):
        #     for b in range(len(var_list)):
        #         v = v + '?,'
        #         # if b == len(var_list) - 2:
        #         # if b == len(var_list) - 1:
        #         #     v = v + '?'
        #     v = re.sub(',$', '', v)
        #     c.executemany('INSERT INTO Result (' + select_var + ') values (' + v + ')', results)
        #     # save the results of SQL query into a RD table
        #     # for result in results:
        #     #     v = ''
        #     #     for element in result:
        #     #         v += element + ', '
        #     #     v = re.sub(', $', '', v)
        #     #     c.execute('INSERT INTO Result (' + select_var + ') values (' + v + ')')
        #     cur = c.cursor()
        #     return cur
        # cu = uri_db(uri_database, self.var_list)
        #
        # # test_query = 'SELECT * FROM Result;'  # debug 20230323
        # # test_results = cursor.execute(test_query).fetchall()  # debug 20230323
        #
        # def build_query(cursor, r_list, sql_tquery):
        #     # by searching ID with ID->URI conversion table, convert ID to URI
        #     select_var2 = ''
        #     for r_l, var in zip(r_list, self.var_list):
        #         if r_l:
        #             select_var2 += r_l + ', '
        #         else:
        #             select_var2 += var + ', '  # 2023/5/18
        #     select_var2 = re.sub(', $', '', select_var2)
        #
        #     # exe_query = 'SELECT ' + select_var2 + ' FROM (Result) '
        #     exe_query = 'SELECT DISTINCT ' + select_var2 + ' FROM (Result) '  # 20230323
        #     # match 's' against Results and at the same time 's' and URI against PREFIX***
        #     for sql_tq in sql_tquery:
        #         if sql_tq != ' ;':  # 2023/5/18
        #             # exe_query = exe_query + ' NATURAL JOIN (' + SQL_tquery[i] + ')'
        #             exe_query = exe_query + ' NATURAL LEFT JOIN (' + sql_tq + ')'  # 20230323
        #     exe_query = exe_query.replace(';', '') + ';'
        #     print(exe_query)
        #     sql_results = cursor.execute(exe_query).fetchall()
        #     return sql_results
        # sparql_results = build_query(cu, r_list, sql_tquery)
        #
        # sparql_results = [list(pp) for pp in sparql_results]  # convert to a list
        sparql_results = []
        for result in results:
            row = []
            for element in result:
                converted_element = element
                try:
                    converted_element = uri.uri_dict_all[element]
                except KeyError:
                    pass
                row.append(converted_element)
            sparql_results.append(row)
        return sparql_results
