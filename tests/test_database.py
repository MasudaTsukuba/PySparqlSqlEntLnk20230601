import sqlite3
import os

working_dir = os.getcwd()
if working_dir.endswith('src'):
    working_dir = os.path.dirname(working_dir)


def test_sql():
    conn = sqlite3.connect(working_dir+'/data_set2/landmark.db')
    cursor = conn.cursor()
    sql = 'SELECT * FROM hotel;'
    cursor.execute(sql)
    results = cursor.fetchall()
    print(results)


if __name__ == '__main__':
    test_sql()
