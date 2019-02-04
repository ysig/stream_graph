"""Test file for time set."""
from pandas import DataFrame
from stream_graph import TimeSetDF
from stream_graph import ITimeSetS
from stream_graph.exceptions import UnrecognizedTimeSet


def test_time_set_df():
    times_a = [(1, 3), (5, 7), (6, 10)]
    tsa = TimeSetDF(times_a, disjoint_intervals=False)

    assert bool(times_a)
    assert not bool(TimeSetDF({}))
    assert not bool(TimeSetDF())

    assert list(tsa) == [(1, 3), (5, 10)]

    assert tsa.size == 7
    assert TimeSetDF().size == 0

    assert 2 in tsa
    assert 1.5, 2.3 in tsa
    assert 2.4, 5.2 not in tsa
    assert 4 not in tsa
    assert (5, 10) in tsa

    assert isinstance(tsa.df, DataFrame)
    assert list(tsa.df.columns) == ['ts', 'tf']

    times_b = [(2, 4), (6, 11)]
    tsb = TimeSetDF(times_b)
    assert list(tsb & tsa) == list(tsa & tsb)
    assert (tsb & tsa).size == 5
    assert list((tsb & tsa)) == sorted([(2, 3), (6, 10)])

    assert list(tsb | tsa) == list(tsa | tsb)
    assert (tsb | tsa).size == 9
    assert list((tsb | tsa)) == sorted([(1, 4), (5, 11)])

    assert list(tsb - tsa) == sorted([(3, 4), (10, 11)])
    assert (tsb - tsa).size == 2
    assert list(tsa - tsb) == sorted([(1, 2), (5, 6)])
    assert (tsb - tsa).size == 2

    assert tsa.issuperset(tsa & tsb)
    assert tsb.issuperset(tsa & tsb)
    assert (tsa | tsb).issuperset(tsa)
    assert (tsa | tsb).issuperset(tsb)
    assert tsa.issuperset(tsa - tsb)

    try:
        tsa | 1
    except UnrecognizedTimeSet:
        pass

    try:
        tsa & 1
    except UnrecognizedTimeSet:
        pass

    try:
        tsa - 1
    except UnrecognizedTimeSet:
        pass

def test_itime_set_s():
    times_a = [(1), (5), (6)]
    tsa = ITimeSetS(times_a)

    assert bool(times_a)
    assert not bool(ITimeSetS({}))
    assert not bool(ITimeSetS())

    assert list(tsa) == [1, 5, 6]

    assert tsa.size == 0
    assert ITimeSetS().size == 0

    assert 5 in tsa
    assert 6 in tsa
    assert 3 not in tsa

    times_b = [(2), (6)]
    tsb = ITimeSetS(times_b)
    assert set(tsb & tsa) == set(tsa & tsb)
    assert set(tsb & tsa) == {6}

    assert set(tsb | tsa) == set(tsa | tsb)
    assert set(tsb | tsa) == {1, 2, 5, 6}

    assert set(tsb - tsa) == {2}
    assert set(tsa - tsb) == {1, 5}

    assert tsa.issuperset(tsa & tsb)
    assert tsb.issuperset(tsa & tsb)
    assert (tsa | tsb).issuperset(tsa)
    assert (tsa | tsb).issuperset(tsb)
    assert tsa.issuperset(tsa - tsb)

    try:
        tsa | 1
    except UnrecognizedTimeSet:
        pass

    try:
        tsa & 1
    except UnrecognizedTimeSet:
        pass

    try:
        tsa - 1
    except UnrecognizedTimeSet:
        pass


if __name__ == "__main__":
    test_time_set_df()
    test_itime_set_s()
