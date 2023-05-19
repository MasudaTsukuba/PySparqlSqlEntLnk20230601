import sqlite3

dbname = './data_set2/data2.db'
conn = sqlite3.connect(dbname)
cur = conn.cursor()
exe_query = 'SELECT * FROM Hotel;'
results = cur.execute(exe_query).fetchall()
# for result in results:
#     print(result)
# print(len(results))

exe_query = 'SELECT Hotel.h_id, Hotel.name, Hotel_place_in.cn_id, H_country.name FROM Hotel NATURAL JOIN Hotel_place_in, H_country WHERE Hotel.h_id = Hotel_place_in.h_id AND Hotel_place_in.cn_id = H_country.cn_id;'
results = cur.execute(exe_query).fetchall()
for result in results:
    print(result)
print(len(results))

conn.close()
