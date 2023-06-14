import subprocess
from src.DatabaseClass import DataBase
from src.MappingClass import Mapping
from src.SparqlQueryClass import SparqlQuery
from src.UriClass import Uri
from src.OutputClass import Output


class ExecuteQueryClass:
    def __init__(self, path):
        self.path = path

    def query2json(self):  # convert sparql query string into json format
        json_file = self.path.input_query_file.replace('.txt', '.json')  # output json file name
        command = self.path.working_path + '/action_folder/node_modules/sparqljs/bin/sparql-to-json'
        cp = subprocess.run([command, '--strict', self.path.common_query_path + self.path.input_query_file],
                            capture_output=True, text=True)  # , '>', 'query_parse.json'])  # クエリをjson形式に構造化
        if cp.returncode != 0:
            print('Error: sparql-to-json: ', cp.stderr)
            return -1
        with open(self.path.common_query_path + json_file, mode='w') as f:
            f.write(cp.stdout)
        return self.path.common_query_path + json_file

    def execute_query(self, input_file):  # , tables):
        # path = PathClass('data_set2')
        self.path.set_input_query(input_file)
        data_base = DataBase(self.path, 'landmark.db')  # 2023/6/1
        # ------ マッピングデータを使ってSPARQL -> SQL に変換する ----------
        self.path.set_mapping_file('mapping_revised.json')
        mapping_class = Mapping(self.path.mapping_file_path)
        # ------ ユーザから得て, JSON形式に変換したSPARQLを取り込む --------
        uri = Uri(self.path)
        # uri.read_entity_linking(tables)  # 2023/6/5
        uri.read_entity_linking_from_csv()  # 2023/6/14
        query_json = self.query2json()
        sparql_query = SparqlQuery(query_json, uri)
        exe_query = sparql_query.convert_to_sql(mapping_class)  # sparql to intermediate sql
        print(exe_query)  # for debug
        # exe_query = 'SELECT s, name, country_id FROM (SELECT museum_id AS s, "http://www.w3.org/2000/01/rdf-schema#label" AS Var501, name AS name FROM museum UNION SELECT hotel_id AS s, "http://www.w3.org/2000/01/rdf-schema#label" AS Var701, name AS name FROM hotel UNION SELECT building_id AS s, "http://www.w3.org/2000/01/rdf-schema#label" AS Var901, name AS name FROM building UNION SELECT heritage_id AS s, "http://www.w3.org/2000/01/rdf-schema#label" AS Var1101, name AS name FROM heritage) NATURAL JOIN (SELECT heritage.heritage_id AS s, "http://example.com/predicate/country" AS Var1301, country.country_id AS country_id FROM heritage_country, heritage, country WHERE heritage.heritage_id = heritage_country.heritage_id AND heritage_country.country_id = country.country_id AND country.country_id != "<http://example.com/country/id/237>");'  # debug
        sql_results, headers = data_base.execute(exe_query)  # execute sql query
        data_base.close()
        sparql_results = sparql_query.convert_to_rdf(uri, sql_results)  # back to rdf
        Output.save_file(self.path.output_file_path, sparql_results, headers)  # save in a file
        print(len(sparql_results))  # debug info
        return sparql_results  # for tests
