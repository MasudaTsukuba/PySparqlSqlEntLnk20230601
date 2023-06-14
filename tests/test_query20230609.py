from src.ExeuteQueryClass import ExecuteQueryClass
from src.PathClass import PathClass

path = PathClass('dataset20230609')
execute = ExecuteQueryClass(path)


def test_query():
    result = execute.execute_query('3_q1.txt')
    assert len(result) == 4
    result = execute.execute_query('3_q2.txt')
    assert len(result) == 3
    result = execute.execute_query('3_q3.txt')
    assert len(result) == 3
