from src.main030_execute_query import execute_query


def test_q1():
    result = execute_query('q1.txt')
    assert len(result) == 801
    result = execute_query('q1_hotel_name_only.txt')  # q1 without country, only hotel id and name
    assert len(result) == 822
    result = execute_query('q1_hotel_country_id.txt')  # q1 wit country_id, but without country name
    assert len(result) == 801
    result = execute_query('q1_pred_of_hotel.txt')  # get predicate of rdf:type as a variable
    assert len(result) == 801
    result = execute_query('q1pred_get_hotel.txt')  # return predicate value
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
    assert len(result) == 19533  # 19526  # 2023/6/1


def test_q5():
    result = execute_query('q5.txt')  # with filter
    assert len(result) == 43999  # 43980  # 2023/6/1
    result = execute_query('q5b.txt')  # without a filter
    assert len(result) == 43999
    result = execute_query('q5c.txt')  # without a filter, with country_id
    assert len(result) == 43999


def test_q6():
    result = execute_query('q6.txt')
    assert len(result) == 219


def test_q7():
    result = execute_query('q7.txt')
    assert len(result) == 0


def test_q1pred_hotel():
    result = execute_query('q1pred_hotel.txt')
    assert len(result) == 801


def test_q1pred_build():
    result = execute_query('q1pred_build.txt')
    assert len(result) == 18470


def test_q1pred_museum():
    result = execute_query('q1pred_museum.txt')
    assert len(result) == 19533  # 19526  # 2023/6/1


def test_q1pred_heritage():
    result = execute_query('q1pred_heritage.txt')
    assert len(result) == 5195  # 5191  # 2023/6/1


# get types of nay entities
def test_query_type_object_hotel20230518():
    query = 'query_type_object20230518.txt'
    result = execute_query(query)
    assert len(result) == 44678  # 45239  # 2023/6/1


def test_query_extract_hotels():
    result = execute_query('query_extract_hotels20230519.txt')
    assert len(result) == 822


def test_query_extract_hotels_with_name():
    result = execute_query('query_extract_hotels_with_name20230519.txt')
    assert len(result) == 822


def test_query_extract_buildings():
    result = execute_query('query_extract_buildings20230519.txt')
    assert len(result) == 18556


def test_query_extract_museums():
    result = execute_query('query_extract_museums20230519.txt')
    assert len(result) == 19958


def test_query_extract_heritages():
    result = execute_query('query_extract_heritages20230519.txt')
    assert len(result) == 5154


# extract all entities that have labels
def test_query_extract_labels():
    result = execute_query('query_extract_labels20230519.txt')
    assert len(result) == 44490
