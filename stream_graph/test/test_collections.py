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

if __name__ == "__main__":
    test_merge()
