"""Test file for link set."""
from pandas import DataFrame
from stream_graph import LinkSetDF
from stream_graph.exceptions import UnrecognizedLinkSet
from stream_graph.exceptions import UnrecognizedDirection
from operator import itemgetter
from nose.tools import assert_equal

def sl(obj):
    return sorted(list(obj))

def test_link_set_df():
    links_a = [(1, 2), (2, 3), (3, 1), (1, 2)]
    lsa = LinkSetDF(links_a, no_duplicates=False)

    assert bool(lsa)
    assert not bool(LinkSetDF({}))
    assert not bool(LinkSetDF())

    assert_equal(lsa.size, 3)
    assert_equal(LinkSetDF().size, 0)

    assert (1, 2) in lsa
    assert (2, 3) in lsa
    assert (3, 1) in lsa
    assert (1, 4) not in lsa

    assert isinstance(lsa.df, DataFrame)
    assert_equal(list(lsa.df.columns), ['u', 'v'])
    assert_equal(list(lsa), sl(set(links_a)))

    links_b = {(2, 3), (3, 1), (1, 4), (2, 3)}
    lsb = LinkSetDF(links_b, no_duplicates=False)
    assert_equal(sl(lsb & lsa), sl([(3, 1), (2, 3)]))
    assert_equal(sl(lsb & lsa), sl(lsa & lsb))
    assert_equal((lsb & lsa).size, 2)

    assert_equal(sl(lsb | lsa), sl(lsa | lsb))
    assert_equal((lsb | lsa).size, 4)
    assert_equal(sl(lsb | lsa), sorted([(1, 2), (1, 4), (2, 3), (3, 1)]))

    assert_equal(sl(lsb - lsa), sorted([(1, 4)]))
    assert_equal((lsb - lsa).size, 1)
    assert_equal(sl(lsa - lsb), sorted([(1, 2)]))
    assert_equal((lsb - lsa).size, 1)

    assert_equal(lsa.degree(1, 'out'), 1)
    assert_equal(lsa.degree(1, 'in'), 1)
    assert_equal(lsa.degree(1, 'both'), 2)

    assert_equal(dict(lsa.degree(direction='out')), {1: 1, 2: 1, 3: 1})
    assert_equal(dict(lsa.degree(direction='in')), {2: 1, 3: 1, 1: 1})
    assert_equal(dict(lsa.degree(direction='both')), {1: 2, 2: 2, 3: 2})

    assert_equal(set(lsa.neighbors_of(1, 'out')), {2})
    assert_equal(set(lsa.neighbors_of(1, 'in')), {3})
    assert_equal(set(lsa.neighbors_of(1, 'both')), {2, 3})
    assert_equal(sorted(list((a, set(b)) for (a, b) in lsa.neighbors_of(direction='out')), key=itemgetter(0)), sorted([(1, {2}), (2, {3}), (3, {1})], key=itemgetter(0)))
    assert_equal(sorted(list((a, set(b)) for (a, b) in lsa.neighbors_of(direction='in')), key=itemgetter(0)), sorted([(2, {1}), (3, {2}), (1, {3})], key=itemgetter(0)))
    assert_equal(sorted(list((a, set(b)) for (a, b) in lsa.neighbors_of(direction='both')), key=itemgetter(0)), sorted([(1, {2, 3}), (2, {1, 3}), (3, {1, 2})], key=itemgetter(0)))

    try:
        lsa.degree(1, 'tvod')
    except UnrecognizedDirection:
        pass

    try:
        lsa.neighbors_of(1, 'tvod')
    except UnrecognizedDirection:
        pass

    assert lsa.issuperset(lsa & lsb)
    assert lsb.issuperset(lsa & lsb)
    assert (lsa | lsb).issuperset(lsa)
    assert (lsa | lsb).issuperset(lsb)
    assert lsa.issuperset(lsa - lsb)

    try:
        lsa | 1
    except UnrecognizedLinkSet:
        pass

    try:
        lsa & 1
    except UnrecognizedLinkSet:
        pass

    try:
        lsa - 1
    except UnrecognizedLinkSet:
        pass

def test_link_set_weighted():
    links_a = [(1, 2, 1), (2, 3, 1), (3, 1, 1), (1, 2, 1)]
    lsa = LinkSetDF(links_a, no_duplicates=False, weighted=True)

    assert bool(lsa)
    assert not bool(LinkSetDF({}, weighted=True))
    assert not bool(LinkSetDF(weighted=True))

    assert_equal(lsa.size, 3)
    assert_equal(lsa.weighted_size, 4)
    assert_equal(LinkSetDF(weighted=True).size, 0)

    assert (1, 2) in lsa
    assert (2, 3) in lsa
    assert (3, 1) in lsa
    assert (1, 4) not in lsa

    assert isinstance(lsa.df, DataFrame)
    assert_equal(list(lsa.df.columns), ['u', 'v', 'w'])
    assert_equal(sl(lsa), sorted([(1, 2, 2), (2, 3, 1), (3, 1, 1)]))

    links_b = [(2, 3, 1), (3, 1, 2), (1, 4, 2), (2, 3, 1)]
    lsb = LinkSetDF(links_b, no_duplicates=False, weighted=True)

    assert_equal(sl(lsb & lsa), sl(lsa & lsb))
    assert_equal((lsb & lsa).size, 2)
    assert_equal(sl((lsb & lsa)), sorted([(2, 3, 3), (3, 1, 3)]))

    assert_equal(sl(lsb | lsa), sl(lsa | lsb))
    assert_equal((lsb | lsa).size, 4)
    assert_equal(sl((lsb | lsa)), sorted([(1, 2, 2), (1, 4, 2), (2, 3, 3), (3, 1, 3)]))

    assert_equal(sl(lsb - lsa), sorted([(1, 4, 2)]))
    assert_equal((lsb - lsa).size, 1)
    assert_equal(sl(lsa - lsb), sorted([(1, 2, 2)]))
    assert_equal((lsb - lsa).size, 1)

    assert_equal(lsa.degree(1, 'out', weights=True), 2)
    assert_equal(lsa.degree(1, 'in', weights=True), 1)
    assert_equal(lsa.degree(1, 'both', weights=True), 3)

    assert_equal(dict(lsa.degree(direction='out', weights=True)), {1: 2, 2: 1, 3: 1})
    assert_equal(dict(lsa.degree(direction='in', weights=True)), {2: 2, 3: 1, 1: 1})
    assert_equal(dict(lsa.degree(direction='both', weights=True)), {1: 3, 2: 3, 3: 2})

    assert_equal(lsa.degree(1, 'out'), 1)
    assert_equal(lsa.degree(1, 'in'), 1)
    assert_equal(lsa.degree(1, 'both'), 2)

    assert_equal(dict(lsa.degree(direction='out')), {1: 1, 2: 1, 3: 1})
    assert_equal(dict(lsa.degree(direction='in')), {2: 1, 3: 1, 1: 1})
    assert_equal(dict(lsa.degree(direction='both')), {1: 2, 2: 2, 3: 2})

    assert_equal(set(lsa.neighbors_of(1, 'out')), {2})
    assert_equal(set(lsa.neighbors_of(1, 'in')), {3})
    assert_equal(set(lsa.neighbors_of(1, 'both')), {2, 3})
    assert_equal(sorted(list((a, set(b)) for (a, b) in lsa.neighbors_of(direction='out')), key=itemgetter(0)), sorted([(1, {2}), (2, {3}), (3, {1})], key=itemgetter(0)))
    assert_equal(sorted(list((a, set(b)) for (a, b) in lsa.neighbors_of(direction='in')), key=itemgetter(0)), sorted([(2, {1}), (3, {2}), (1, {3})], key=itemgetter(0)))
    assert_equal(sorted(list((a, set(b)) for (a, b) in lsa.neighbors_of(direction='both')), key=itemgetter(0)), sorted([(1, {2, 3}), (2, {1, 3}), (3, {1, 2})], key=itemgetter(0)))

    links_b = {(2, 3), (3, 1), (1, 4), (2, 3)}
    lsb = LinkSetDF(links_b, no_duplicates=False)
    assert_equal(sl(lsb & lsa), sl(lsa & lsb))
    assert_equal((lsb & lsa).size, 2)
    assert_equal(sl((lsb & lsa)), sorted([(2, 3), (3, 1)]))

    assert_equal(sl(lsb | lsa), sl(lsa | lsb))
    assert_equal((lsb | lsa).size, 4)
    assert_equal(sl((lsb | lsa)), sl([(1, 2), (1, 4), (2, 3), (3, 1)]))

    assert_equal(sl(lsb - lsa), sl([(1, 4)]))
    assert_equal((lsb - lsa).size, 1)
    assert_equal(sl(lsa - lsb), sorted([(1, 2)]))
    assert_equal((lsb - lsa).size, 1)


if __name__ == "__main__":
    test_link_set_df()
    test_link_set_weighted()
