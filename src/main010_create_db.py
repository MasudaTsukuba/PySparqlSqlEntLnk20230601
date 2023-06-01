# create landmark.db in data_set2 folder
# 2023/6/1, Tadashi Masuda

# import os
# import sqlite3
# import csv
from src.PathClass import PathClass
from src.DatabaseClass import DataBase
#
# # working_path = os.getcwd()
# # if working_path.endswith('src'):
# #     working_path = os.path.dirname(working_path)
# path = PathClass('')
# database_path = path.working_path+'/data_set2/'
#
#
# def create_database():
#     conn = sqlite3.connect(database_path+'landmark.db')
#     conn.commit()
#     conn.close()
#
#
# def create_table():
#     conn = sqlite3.connect(database_path+'landmark.db')
#     cursor = conn.cursor()
#     tables = [
#         "build", "buildinc",
#         "hotel", "hotel_place_in",
#         "museum", "museumincountry",
#         "heritage", "heritage_placein",
#         "country"
#     ]
#     for table in tables:
#         sql = "DROP TABLE " + table + ";"
#         try:
#             cursor.execute(sql)
#         except:
#             print('DROP FAILED: ' + table)
#             pass
#         # finally:
#         #     if cnx:
#         #         cnx.close()
#
#     sqls = [
#         "CREATE TABLE build (b_id VARCHAR(255) PRIMARY KEY, name VARCHAR(255), comment VARCHAR(255));",
#         "CREATE TABLE buildinc (b_id VARCHAR(255), bc_id VARCHAR(255), FOREIGN KEY (b_id) REFERENCES build(b_id), FOREIGN KEY (bc_id) REFERENCES country(country_id));",
#         "CREATE TABLE hotel (h_id VARCHAR(255) PRIMARY KEY, name VARCHAR(255), comment VARCHAR(255));",
#         "CREATE TABLE hotel_place_in (h_id VARCHAR(255), cn_id VARCHAR(255), FOREIGN KEY (h_id) REFERENCES hotel(h_id), FOREIGN KEY (cn_id) REFERENCES country(country_id));",
#         "CREATE TABLE Museum (museum_iD VARCHAR(255) PRIMARY KEY, name VARCHAR(255), comment VARCHAR(255));",
#         "CREATE TABLE museumincountry (museum_iD VARCHAR(255), co_id VARCHAR(255), FOREIGN KEY (museum_iD) REFERENCES Museum(museum_iD), FOREIGN KEY (co_id) REFERENCES country(country_id));",
#         "CREATE TABLE heritage (p_id VARCHAR(255) PRIMARY KEY, name VARCHAR(255), comment VARCHAR(255));",
#         "CREATE TABLE heritage_placein (p_id VARCHAR(255), c_id VARCHAR(255), FOREIGN KEY (p_id) REFERENCES heritage(p_id), FOREIGN KEY (c_id) REFERENCES country(country_id));",
#         "CREATE TABLE country (country_id VARCHAR(255) PRIMARY KEY, country_name VARCHAR(255), country_comment VARCHAR(255));"
#     ]
#
#     for sql in sqls:
#         try:
#             cursor.execute(sql)
#             print('CREATE TABLE SUCCEEDED: ' + sql)
#             pass
#         except:
#             print('CREATE TABLE FAILED: ' + sql)
#             pass
#         # finally:
#         #     if cnx:
#         #         cnx.close()
#
#     cursor.close()
#     conn.commit()
#     conn.close()
#
#
# def insert_data():
#     conn = sqlite3.connect(database_path+'landmark.db')
#     cursor = conn.cursor()
#     tables = [
#         "build", "buildinc",
#         "hotel", "hotel_place_in",
#         "Museum", "museumincountry",
#         "heritage", "heritage_placein",
#         "country"
#     ]
#
#     path_tables = [
#         "Building/Build", "Building/buildinC",
#         "Hotel/Hotel", "Hotel/Hotel_place_in",
#         "Museum/Museum", "Museum/museumIncountry",
#         "Heritage/heritage", "Heritage/Heritage_placein",
#         "Country/Country",
#     ]
#     sqls = [
#         "INSERT INTO build (b_id, name, comment) VALUES (?, ?, ?)",
#         "INSERT INTO buildinc (b_id, bc_id) VALUES (?, ?)",
#         "INSERT INTO hotel (h_id, name, comment) VALUES (?, ?, ?)",
#         "INSERT INTO hotel_place_in (h_id, cn_id) VALUES (?, ?)",
#         "INSERT INTO Museum (museum_id, name, comment) VALUES (?, ?, ?)",
#         "INSERT INTO museumincountry (museum_id, co_id) VALUES (?, ?)",
#         "INSERT INTO heritage (p_id, name, comment) VALUES (?, ?, ?)",
#         "INSERT INTO heritage_placein (p_id, c_id) VALUES (?, ?)",
#         "INSERT INTO country (country_id, country_name, country_comment) VALUES (?, ?, ?)"
#     ]
#
#     for table, path_table, sql in zip(tables, path_tables, sqls):
#         file = database_path + path_table + ".csv"
#         print(path_table)
#         with open(file, 'r') as csvfile:
#             reader = csv.reader(csvfile)
#             first = True
#             for row in reader:
#                 if first:
#                     first = False
#                 else:
#                     try:
#                         # print(row)
#                         cursor.execute(sql, row)
#                     except:
#                         pass
#             conn.commit()
#     cursor.close()
#     conn.close()


if __name__ == '__main__':
    path = PathClass('')
    db = DataBase(path, 'data_set2', 'landmark.db')
    db.create_database()
    db.create_table()
    db.insert_data()
