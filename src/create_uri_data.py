import os
import csv
import sqlite3


column_dict = {'PREFIX_Build': 'URI_Build', 'PREFIX_hotel': 'URI_hotel', 'PREFIX_museum': 'URI_museum', 'PREFIX_WH': 'URI_WH'}


def csv_to_sqlite(csv_file, db_file, table_name):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    with open(csv_file, 'r') as file:
        csv_reader = csv.reader(file)
        header = ['ID', csv_file.replace('./data_set2/URI/', '').replace('.csv', '').replace('PREFIX', 'URI')]  # next(csv_reader)
        columns = [f'{column} TEXT' for column in header]
        try:
            drop_table_query = f'DROP TABLE {table_name}'
            cursor.execute(drop_table_query)
        except:
            pass
        create_table_query = f'CREATE TABLE {table_name} ({", ".join(columns)})'
        cursor.execute(create_table_query)
        insert_query = f'INSERT INTO {table_name} VALUES ({", ".join(["?"] * len(header))})'
        for row in csv_reader:
            cursor.execute(insert_query, row)
    conn.commit()
    conn.close()


uri_path = '../data_set2/URI/'
files = os.listdir(uri_path)
for file in files:
    if file.startswith('PREFIX'):
        print(file)
        csv_file = uri_path+file
        db_file = uri_path+'URI_data.db'
        table_name = file.replace('.csv', '')
        csv_to_sqlite(csv_file, db_file, table_name)
pass
