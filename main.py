import json
import csv
import sqlite3
import trans_sql
import time

query_URI = 'query.json'
output = 'output.csv'
dbname = './data_set2/data2.db'
mapping_URI = './data_set2/mapping/mapping.json'
URI_directory = './data_set2/URI/'


# ------ ユーザから得て, JSON形式に変換したSPARQLを取り込む --------
json_open = open(query_URI, 'r')
query_dict = json.load(json_open)
json_open.close()
# ------------------------------------------------------------

# ------ マッピングデータを使ってSPARQL -> SQL に変換する ----------

#マッピングデータの取り込み
json_open = open(mapping_URI, 'r')
mapping_dict = json.load(json_open)
json_open.close()

#出力する変数リストを作成
var_list = []
for i in range(len(query_dict['variables'])):
    var_list.append(query_dict['variables'][i]['value'])

#FILTERの条件リストを作成
filter_list = []
for i in range(len(query_dict['where'])):
    if(query_dict['where'][i]['type'] == 'filter'):
        prefix = query_dict['where'][i]['expression']['args']
        filter_list.append([prefix[0]['value'],prefix[0]['value'] + ' ' + query_dict['where'][i]['expression']['operator'] + ' "' + prefix[1]['value'] + '"'])

#print(var_list)
#print(filter_list)



#SPARQLクエリの各トリプルパターンから候補のSQLクエリを検索
SQL_query = []
transURI_list = []
check = []
checked = []
sparql_to_sql_s = time.time()
for i in range(len(query_dict['where'][0]['triples'])):
    SQL_subquery = []
    q_predicate = query_dict['where'][0]['triples'][i]['predicate']['value']
    for j in range(len(mapping_dict)):
        predicate = mapping_dict[j]["predicate"]

        if(q_predicate == predicate):
            sql = mapping_dict[j]["SQL"]
            query = query_dict['where'][0]['triples'][i]
            mapping = mapping_dict[j]
            answer = trans_sql.f(sql,query,mapping,filter_list)
            re_sql = answer[0]
            if(answer[1] != []):
                for k in range(len(answer[1])):
                    if answer[1][k][0] not in checked:
                        transURI_list.append(answer[1][k])
                        check.append(answer[1][k][0])
                
            if(re_sql != 'No'):
                SQL_subquery.append(re_sql)

    insert_SQL = ''
    if(len(SQL_subquery)!= 0):
        for k in range(len(SQL_subquery)):
            insert_SQL = insert_SQL + SQL_subquery[k]
            if(k != len(SQL_subquery) - 1):
                insert_SQL = insert_SQL + ' UNION '
        insert_SQL = insert_SQL.replace(';','') + ';'

    checked = checked + check
    #print(transURI_list)

    SQL_query.append(insert_SQL)

sparql_to_sql_e = time.time()

#print(SQL_query)

# ------------------------------------------------------------

# --------- 各SQLクエリを実行 ----------------------------------

conn = sqlite3.connect(dbname)

# SQLiteを操作するためのカーソルを作成
cur = conn.cursor()

# それぞれのクエリの実行し、そのクエリ結果や結果のヘッダーを格納
select_var = ''

for i in range(len(var_list)):
    if(i != len(var_list)-1):
        select_var = select_var + var_list[i] + ', '
    else:
        select_var = select_var + var_list[i]

exe_query = 'SELECT ' + select_var + ' FROM '

for i in range(len(SQL_query)):
    if(i != len(SQL_query)-1):
        exe_query = exe_query + ' (' +SQL_query[i] + ') NATURAL JOIN '
    else:
        exe_query = exe_query + ' (' + SQL_query[i] + ')'

exe_query = exe_query.replace(';','') + ';'

#print(exe_query)

q_start = time.time()
results = cur.execute(exe_query).fetchall()
q_end = time.time()

headers = [col[0] for col in cur.description]

# --------- SQLクエリ結果をSPARQLクエリ結果に合わせるため、必要に応じて文字列->URIに変換する ----------------------------------
transURI_list = list(set(tuple(i) for i in transURI_list))
transURI_list = [list(i) for i in transURI_list]
#print(transURI_list)

results = [list(i) for i in results]

#print(results)

#結果の表示, output.csvに出力される

#print(transURI_list)

t_start = time.time()

for i in range(len(headers)):
    for transURI in transURI_list:

        if((headers[i] == transURI[0]) & (transURI[1] != 'plain')):
            with open(URI_directory + transURI[1] + '.csv') as g:
                reader = csv.reader(g)
                for row in reader:
                    for j in range(len(results)):
                        if(results[j][i] == row[0]):
                            results[j][i] = row[1]
                            break

t_end = time.time()

with open(output, mode='w') as file:
    writer = csv.writer(file)
    writer.writerow(headers)
    writer.writerows(results)


print('SPRARQL to SQL time: {0}'.format(sparql_to_sql_e - sparql_to_sql_s))
print('SQL exe time: {0}'.format(q_end - q_start))
print('URI trans time: {0}'.format(t_end - t_start))