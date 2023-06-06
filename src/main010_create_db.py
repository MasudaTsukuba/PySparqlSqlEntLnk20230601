# create landmark.db in data_set2 folder
# 2023/6/1, Tadashi Masuda

from src.PathClass import PathClass
from src.DatabaseClass import DataBase


if __name__ == '__main__':
    path = PathClass('')
    db = DataBase(path, 'data_set2', 'landmark.db')
    db.create_database()
    db.create_table()
    db.insert_data()
