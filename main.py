from MappingClass import Mapping
from DatabaseClass import DataBase
from SparqlQueryClass import SparqlQuery
from UriClass import Uri
from OutputClass import Output
import subprocess


path = '/home/masuda/PycharmProjects/PySparqlQuery20230508/'
working_dir = '/home/masuda/PycharmProjects/PySparqlSatoNew20230509/'


def query2json(input_file):
    json_file = input_file.replace('.txt', '.json')
    command = working_dir + 'action_folder/node_modules/sparqljs/bin/sparql-to-json'
    cp = subprocess.run([command, '--strict', path + input_file],
                        capture_output=True, text=True)  # , '>', 'query_parse.json'])  # クエリをjson形式に構造化
    if cp.returncode != 0:
        print('Error: sparql-to-json: ', cp.stderr)
        return -1
    with open(working_dir+json_file, mode='w') as f:
        f.write(cp.stdout)
    return working_dir+json_file


def execute_query(input_file):
    # query_URI = 'query.json'
    # query_URI = 'query/q1.json'  # #################2023/3/20
    # query_uri = 'query/q2.json'  # #################2023/3/20
    # output = 'output.csv'
    # output = 'q1.csv'  # #################2023/3/20
    output_file_name = 'q2.csv'  # #################2023/3/20
    # db_name = './data_set2/data2.db'
    # mapping_URI = './data_set2/mapping/mapping.json'
    # URI_directory = './data_set2/URI/'
    uri_database = './data_set2/URI/URI_data.db'
    # uri_mapping = './data_set2/URI/uri_mapping.json'

    data_base = DataBase('./data_set2/data2.db')
    # ------ マッピングデータを使ってSPARQL -> SQL に変換する ----------
    mapping_class = Mapping()
    # ------ ユーザから得て, JSON形式に変換したSPARQLを取り込む --------
    uri = Uri('./data_set2/URI/')
    # input_file = 'query/q1.json'
    # sparql_query = SparqlQuery('query/q2.json', uri)
    sparql_query = SparqlQuery(query2json(input_file), uri)
    # sparql_query = SparqlQuery('query/q7.json', uri)
    exe_query = sparql_query.convert_to_sql(mapping_class)  # sparql to intermediate sql
    print(exe_query)
    sql_results, headers = data_base.execute(exe_query)  # execute sql query
    data_base.close()
    # results = [('15923583h', 'Conrad Hong Kong', "People's Republic of Chine")]  # debug, 20230323
    sparql_results = sparql_query.convert_to_rdf(uri_database, sql_results)  # back to rdf
    # Output.save_file(output_file_name, sparql_results, headers)  # save in a file
    Output.save_file(input_file.replace('query/', 'output/').replace('.json', '.csv'),
                     sparql_results, headers)  # save in a file
    print(len(sparql_results))
    return sparql_results


if __name__ == '__main__':
    # execute_query('query/q1.json')
    # execute_query('query/q1.txt')
    # query = 'query/q1pred_hotel.txt'
    # query = 'query/q1pred_build.txt'
    # query = 'query/q1pred_museum.txt'
    # query = 'query/q1pred_heritage.txt'
    query = 'query/query_type_object_hotel20230518.txt'
    execute_query(query)
