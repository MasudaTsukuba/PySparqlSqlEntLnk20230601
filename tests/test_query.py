from src.main import execute_query


def test_q1():
    result = execute_query('q1.txt')
    assert len(result) == 801


def test_q2():
    result = execute_query('q2.txt')
    assert len(result) == 2146


def test_q3a():
    result = execute_query('q3a.txt')
    assert len(result) == 3732


def test_q3b():
    result = execute_query('q3b.txt')
    assert len(result) == 3695  # 2023/5/8 ????


def test_q4():
    result = execute_query('q4.txt')
    assert len(result) == 19526


def test_q5():
    result = execute_query('q5.txt')
    assert len(result) == 43980


def test_q6():
    result = execute_query('q6.txt')
    assert len(result) == 219


def test_q7():
    result = execute_query('q7.txt')
    assert len(result) == 1


def test_q1pred_hotel():
    result = execute_query('q1pred_hotel.txt')
    assert len(result) == 801
