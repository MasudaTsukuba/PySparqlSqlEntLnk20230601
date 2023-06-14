from src.ExeuteQueryClass import ExecuteQueryClass
from src.PathClass import PathClass


if __name__ == '__main__':
    path = PathClass('dataset20230609')
    execute = ExecuteQueryClass(path)
    query = '3_q1.txt'
    query = '3_q2.txt'
    query = '3_q3.txt'
    # tables = ['country', 'building']
    execute.execute_query(query)  # , tables)
