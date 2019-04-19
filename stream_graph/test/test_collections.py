"""Test file for stream graph."""
from stream_graph.collections import TimeCollection

def test_merge():
    # Continuous
    def add(x, y):
        return x + y

    tc_a = TimeCollection([(1, 4), (2, 3), (4, 5), (5, 0), (6, 1), (7, 0)])
    tc_b = TimeCollection([(1.5, 1), (2, 2), (3, 1), (5.5, 0), (8, 1)])
    assert list(tc_a.merge(tc_b, add, missing_value=0)) == list(tc_b.merge(tc_a, add, missing_value=0))
    assert list(tc_a.merge(tc_b, add, missing_value=0)) == [(1, 4), (1.5, 5), (2, 5), (3, 4), (4, 6), (5, 1), (5.5, 0), (6, 1), (7, 0), (8, 1)]
    assert not tc_a.merge(tc_b, add, missing_value=0).instants

    tc_a = TimeCollection([(1, 4), (2, 3), (4, 5), (5, 0), (6, 1), (7, 0)])
    tc_b = TimeCollection([(1.5, 1), (2, 2), (3, 1), (5.5, 0), (8, 1)], True)
    assert list(tc_a.merge(tc_b, add, missing_value=0)) == list(tc_b.merge(tc_a, add, missing_value=0))
    assert list(tc_a.merge(tc_b, add, missing_value=0)) == [(1.5, 5), (2, 5), (3, 4), (5.5, 0), (8, 1)]
    assert not tc_a.merge(tc_b, add, missing_value=0).instants
    
    tc_a = TimeCollection([(1, 4), (2, 3), (4, 5), (5, 0), (6, 1), (7, 0)], True)
    tc_b = TimeCollection([(1.5, 1), (2, 2), (3, 1), (5.5, 0), (8, 1)], True)
    assert list(tc_a.merge(tc_b, add, missing_value=0)) == list(tc_b.merge(tc_a, add, missing_value=0))
    assert list(tc_a.merge(tc_b, add, missing_value=0)) == [(1, 4), (1.5, 1), (2, 5), (3, 1), (4, 5), (5, 0), (5.5, 0), (6, 1), (7, 0), (8, 1)]
    assert tc_a.merge(tc_b, add, missing_value=0).instants

def test_search():
    a = TimeCollection([(1, 3), (4, 4)])
    assert a.search_time(0) == 0
    assert a.search_time(1) == 0
    assert a.search_time(2) == 0
    assert a.search_time(3) == 0
    assert a.search_time(4) == 1
    assert a.search_time(5) == 1

    a = TimeCollection([(1, 3), (4, 4)], True)
    assert a.search_time(0) is None
    assert a.search_time(1) == 0
    assert a.search_time(2) is None
    assert a.search_time(3) is None
    assert a.search_time(4) == 1
    assert a.search_time(5) is None


if __name__ == "__main__":
    test_search()
    test_merge()
