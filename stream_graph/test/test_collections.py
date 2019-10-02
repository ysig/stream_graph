"""Test file for stream graph."""
from nose.tools import assert_equal
from stream_graph.collections import TimeCollection


def add(x, y):
    return x + y


def test_merge():
    # Continuous
    tc_a = TimeCollection([((1, True), 4), ((2, True), 3), ((4, False), 5), ((5, False), 0), ((6, False), 1), ((7, True), 0)])
    tc_b = TimeCollection([((1.5, False), 1), ((2, False), 2), ((3, False), 1), ((5.5, True), 0), ((8, True), 1)])
    assert_equal(list(tc_a.merge(tc_b, add, missing_value=0)), list(tc_b.merge(tc_a, add, missing_value=0)))
    assert_equal(list(tc_a.merge(tc_b, add, missing_value=0)), [((1, True), 4), ((1.5, False), 5), ((2, False), 6), ((2, True), 5), ((3, False), 4), ((4, False), 6), ((5, False), 1), ((5.5, True), 0), ((6, False), 1), ((7, True), 0), ((8, True), 1)])
    assert not tc_a.merge(tc_b, add, missing_value=0).instants

    # future fix
    # tc_a = TimeCollection([((1, True), 4), ((2, True), 3), ((4, False), 5), ((5, False), 0), ((6, False), 1), ((7, True), 0)])
    # tc_b = TimeCollection([(1.5, 1), (2, 2), (3, 1), (5.5, 0), (8, 1)], instantaneous=True)
    # print(tc_a.merge(tc_b, add, missing_value=0))
    # assert list(tc_a.merge(tc_b, add, missing_value=0)) == list(tc_b.merge(tc_a, add, missing_value=0))
    # assert list(tc_a.merge(tc_b, add, missing_value=0)) == [(1.5, 5), (2, 5), (3, 4), (5.5, 0), (8, 1)]
    # assert not tc_a.merge(tc_b, add, missing_value=0).instants

    # tc_a = TimeCollection([(1, 4), (2, 3), (4, 5), ((5, False), 0), ((6, False), 1), ((7, True), 0)], instantaneous=True)
    # tc_b = TimeCollection([(1.5, 1), (2, 2), (3, 1), (5.5, 0), (8, 1)], instantaneous=True)
    # assert list(tc_a.merge(tc_b, add, missing_value=0)) == list(tc_b.merge(tc_a, add, missing_value=0))
    # assert list(tc_a.merge(tc_b, add, missing_value=0)) == [(1, 4), (1.5, 1), (2, 5), (3, 1), (4, 5), (5, 0), (5.5, 0), (6, 1), (7, 0), (8, 1)]
    # assert tc_a.merge(tc_b, add, missing_value=0).instants

    # Discrete
    tc_a = TimeCollection([(2, 4), (4, 3), (8, 5), (10, 0), (12, 1), (14, 0)], discrete=True)
    tc_b = TimeCollection([(3, 1), (4, 2), (6, 1), (11, 0), (16, 1)], discrete=True)
    assert_equal(list(tc_a.merge(tc_b, add, missing_value=0)), list(tc_b.merge(tc_a, add, missing_value=0)))
    assert_equal(list(tc_a.merge(tc_b, add, missing_value=0)), [(2, 4), (3, 5), (6, 4), (8, 6), (10, 1), (11, 0), (12, 1), (14, 0), (16, 1)])
    assert not tc_a.merge(tc_b, add, missing_value=0).instants

    # tc_a = TimeCollection([(2, 4), (3, 3), (4, 5), (10, 0), (12, 1), (14, 0)], instantaneous=True)
    # tc_b = TimeCollection([(3, 1), (4, 2), (6, 1), (11, 0), (16, 1)], discrete=True, instantaneous=True)
    # assert list(tc_a.merge(tc_b, add, missing_value=0)) == list(tc_b.merge(tc_a, add, missing_value=0))
    # assert list(tc_a.merge(tc_b, add, missing_value=0)) == [(1.5, 5), (2, 5), (3, 4), (5.5, 0), (8, 1)]
    # assert not tc_a.merge(tc_b, add, missing_value=0).instants

    tc_a = TimeCollection([(2, 4), (4, 3), (8, 5), (10, 0), (12, 1), (14, 0)], discrete=True, instantaneous=True)
    tc_b = TimeCollection([(3, 1), (4, 2), (6, 1), (11, 0), (16, 1)], discrete=True, instantaneous=True)
    assert_equal(list(tc_a.merge(tc_b, add, missing_value=0)), list(tc_b.merge(tc_a, add, missing_value=0)))
    assert_equal(list(tc_a.merge(tc_b, add, missing_value=0)), [(2, 4), (3, 1), (4, 5), (6, 1), (8, 5), (10, 0), (11, 0), (12, 1), (14, 0), (16, 1)])
    assert tc_a.merge(tc_b, add, missing_value=0).instants


def test_search():
    a = TimeCollection([((1, False), 3), ((4, True), 4)], discrete=False)
    for i, v1, v2 in zip(range(6), 4 * [0] + 2 * [1], 5 * [0] + [1]):
        assert_equal(a.search_time(i), v1)
        assert_equal(a.search_time((i, False)), v2)

    a = TimeCollection([(1, 3), (4, 4)], discrete=True)
    for i, v1 in zip(range(6), 4 * [0] + 2 * [1]):
        assert_equal(a.search_time(i), v1)

    a = TimeCollection([(1, 3), (4, 4)], instantaneous=True)
    for i, v1 in zip(range(6), [None, 0, None, None, 1, None]):
        assert_equal(a.search_time(i), v1)


if __name__ == "__main__":
    test_search()
    test_merge()
