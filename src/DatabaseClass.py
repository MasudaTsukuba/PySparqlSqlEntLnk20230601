import sqlite3


class DataBase:
    def __init__(self, db_name):
        self.conn = sqlite3.connect(db_name)
        self.cur = self.conn.cursor()

    def close(self):
        self.conn.close()

    def execute(self, sql):
        return_list = self.cur.execute(sql).fetchall()
        headers = [col[0] for col in self.cur.description]
        results = [list(i) for i in return_list]
        return results, headers
