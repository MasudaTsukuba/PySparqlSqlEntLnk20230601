import os
import subprocess
from src.MappingClass import Mapping
from src.DatabaseClass import DataBase
from src.SparqlQueryClass import SparqlQuery
from src.UriClass import Uri
from src.OutputClass import Output


working_dir = os.getcwd()
if working_dir.endswith('src'):
    working_dir = os.path.dirname(working_dir)
# common_query_path = os.path.dirname(working_dir)+'/PySparqlQuery20230508/query/'
common_query_path = os.path.dirname(working_dir)+'/PySparqlSatoNew20230509/query/'


def query2json(input_file):  # convert sparql query string into json format
    json_file = input_file.replace('.txt', '.json')  # output json file name
    command = working_dir + '/action_folder/node_modules/sparqljs/bin/sparql-to-json'
    cp = subprocess.run([command, '--strict', common_query_path + input_file],
                        capture_output=True, text=True)  # , '>', 'query_parse.json'])  # クエリをjson形式に構造化
    if cp.returncode != 0:
        print('Error: sparql-to-json: ', cp.stderr)
        return -1
    with open(working_dir+json_file, mode='w') as f:
        f.write(cp.stdout)
    return working_dir+json_file


def execute_query(input_file):
    uri_database = './data_set2/URI/URI_data.db'
    # data_base = DataBase('./data_set2/data2.db')
    data_base = DataBase(working_dir+'/data_set2/landmark.db')  # 2023/5/22
    # ------ マッピングデータを使ってSPARQL -> SQL に変換する ----------
    mapping_class = Mapping(working_dir+'/data_set2/mapping/mapping_revised.json')
    # ------ ユーザから得て, JSON形式に変換したSPARQLを取り込む --------
    uri = Uri(working_dir+'/data_set2/URI/')
    sparql_query = SparqlQuery(query2json(input_file), uri)
    exe_query = sparql_query.convert_to_sql(mapping_class)  # sparql to intermediate sql
    print(exe_query)  # for debug
    sql_results, headers = data_base.execute(exe_query)  # execute sql query
    data_base.close()
    sparql_results = sparql_query.convert_to_rdf(uri, sql_results)  # back to rdf
    Output.save_file(working_dir+'/output/'+input_file.replace('.json', '.csv'),
                     sparql_results, headers)  # save in a file
    print(len(sparql_results))  # debug info
    return sparql_results


if __name__ == '__main__':
    # query = 'q1.txt'
    # query = 'q3b.txt'
    query = 'q5.txt'
    # query = 'q5c.txt'
    # query = 'q7.txt'
    # query = 'q1pred_hotel.txt'
    # query = 'q1pred_build.txt'
    # query = 'q1pred_museum.txt'
    # query = 'q1pred_heritage.txt'
    # query = 'query_type_object_hotel20230518.txt'
    # query = 'query_extract_hotels20230519.txt'
    # query = 'query_extract_museums20230519.txt'
    # query = 'query_extract_buildings20230519.txt'
    # query = 'query_extract_heritages20230519.txt'
    # query = 'query_extract_hotels_with_name20230519.txt'
    # query = 'query_extract_labels20230519.txt'
    query = 'q1pred_get_hotel.txt'
    execute_query(query)
