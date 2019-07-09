"""Test file for time set."""
from stream_graph import TimeSetDF
from stream_graph import ITimeSetS
from stream_graph.exceptions import UnrecognizedTimeSet
from nose.tools import assert_equal


def test_time_set_df():
    for d, do in [(False, 0), (True, 2)]:
        ita, itb, itc = ((tuple(), tuple(), tuple()) if d else (('both',), ('right',), ('left',)))
        times_a = [(1, 3), (5, 7), (6, 10)]
        tsa = TimeSetDF(times_a, disjoint_intervals=False, discrete=d)

        assert bool(times_a)
        assert not bool(TimeSetDF({}))
        assert not bool(TimeSetDF())

        assert_equal(list(tsa), [(1, 3) + ita, (5, 10) + ita])

        assert_equal(tsa.size, 7 + do)
        assert_equal(TimeSetDF().size, 0)

        assert 2 in tsa
        assert 1.5, 2.3 in tsa
        assert 2.4, 5.2 not in tsa
        assert 4 not in tsa
        assert (5, 10) in tsa

        times_b = [(2, 4), (6, 11)]
        tsb = TimeSetDF(times_b, discrete=d)
        assert_equal(list(tsb & tsa), list(tsa & tsb))
        assert_equal((tsb & tsa).size, 5 + do)
        assert_equal(list((tsb & tsa)), sorted([(2, 3) + ita, (6, 10) + ita]))

        assert_equal(list(tsb | tsa), list(tsa | tsb))
        assert_equal((tsb | tsa).size, 9 + do)
        assert_equal(list((tsb | tsa)), sorted([(1, 11)] if d else [(1, 4, 'both'), (5, 11, 'both')]))

        assert_equal(list(tsa - tsb), sorted([(1, 1), (5, 5)] if d else [(1, 2, 'left'), (5, 6, 'left')]))
        assert_equal((tsa - tsb).size, 2)
        assert_equal(list(tsb - tsa), sorted([(4, 4), (11, 11)] if d else [(3, 4, 'right'), (10, 11, 'right')]))
        assert_equal((tsb - tsa).size, 2)

        #assert tsa.issuperset(tsa & tsb)
        #assert tsb.issuperset(tsa & tsb)
        #assert (tsa | tsb).issuperset(tsa)
        #ssert (tsa | tsb).issuperset(tsb)
        #assert tsa.issuperset(tsa - tsb)

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
    for d, s in [(False, 0), (True, 3)]:
        times_a = [(1), (5), (6)]
        tsa = ITimeSetS(times_a, discrete=d)

        assert bool(times_a)
        assert not bool(ITimeSetS({}))
        assert not bool(ITimeSetS())

        assert_equal(list(tsa), [1, 5, 6])

        assert_equal(tsa.size, s)
        assert_equal(ITimeSetS().size, 0)

        assert 5 in tsa
        assert 6 in tsa
        assert 3 not in tsa

        times_b = [(2), (6)]
        tsb = ITimeSetS(times_b, discrete=d)
        assert_equal(set(tsb & tsa), set(tsa & tsb))
        assert_equal(set(tsb & tsa), {6})

        assert_equal(set(tsb | tsa), set(tsa | tsb))
        assert_equal(set(tsb | tsa), {1, 2, 5, 6})

        assert_equal(set(tsb - tsa), {2})
        assert_equal(set(tsa - tsb), {1, 5})

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

    assert ITimeSetS().discrete is None


if __name__ == "__main__":
    test_time_set_df()
    test_itime_set_s()
