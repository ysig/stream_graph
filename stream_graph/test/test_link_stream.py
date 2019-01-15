"""Test file for time set."""
from itertools import product
from pandas import DataFrame
from stream_graph import NodeSetS
from stream_graph import TimeSetDF
from stream_graph import NodeStreamB
from stream_graph import NodeStreamDF
from stream_graph import LinkStreamDF
from stream_graph.exceptions import UnrecognizedLinkStream

def test_link_stream_df():
    df = [(1, 2, 2, 3), (1, 2, 3, 5), (2, 1, 6, 8), (2, 1, 1, 3)]
    lsa = LinkStreamDF(df, disjoint_intervals=False)

    assert bool(lsa)
    assert not bool(LinkStreamDF([]))
    assert not bool(LinkStreamDF())
    assert set(lsa) == {(1, 2, 2, 5), (2, 1, 6, 8), (2, 1, 1, 3)}

    assert lsa.m == 2
    assert lsa.size == 7
    assert LinkStreamDF().size == 0
    assert LinkStreamDF([]).size == 0

    assert (1, 2, None) in lsa
    assert (1, None, 3.4) in lsa
    assert (5, None, None) not in lsa
    assert (1, 3, 3.4) not in lsa
    assert (1, 2, (3.4, 4)) in lsa

    assert lsa.link_duration(1, 2) == 3
    assert lsa.link_duration(2, 1) == 4
    assert lsa.link_duration(2, 1) == lsa.link_duration(1, 2, 'in')
    assert lsa.link_duration(1, 2, direction='both') == 6
    assert lsa.link_duration(5, 1) == 0

    assert set(lsa.links_at(1)) == {(2, 1)}
    assert set(lsa.links_at((2, 2.5))) == {(1, 2), (2, 1)}
    assert set(lsa.links_at(10)) == set()

    assert set(lsa.times_of(1, 2)) == {(2, 5)}
    assert set(lsa.times_of(10, 3)) == set()

    assert lsa.m_at(2) == 2
    assert lsa.m_at(10) == 0

    df = [(1, 2, 1, 4), (1, 2, 6, 7), (2, 1, 2.5, 2.6)]
    lsb = LinkStreamDF(df)
    assert set(lsb & lsa) == set(lsa & lsb)
    assert isinstance(lsa & lsb, LinkStreamDF)
    assert set(lsa & lsb) == {(1, 2, 2, 4), (2, 1, 2.5, 2.6)}
    assert (lsa & lsb).size == 2.1

    assert isinstance(lsa | lsb, LinkStreamDF)
    assert set((lsb | lsa)) == {(2, 1, 1, 3), (2, 1, 6, 8), (1, 2, 1, 5), (1, 2, 6, 7)}
    assert (lsb | lsa).size == 9
    assert set(lsb | lsa) == set(lsa | lsb)

    assert set(lsb - lsa) == {(1, 2, 1, 2), (1, 2, 6, 7)}
    assert (lsb - lsa).size == 2
    assert isinstance(lsb - lsa, LinkStreamDF)
    assert set(lsa - lsb) == {(2, 1, 2.6, 3), (2, 1, 1, 2.5), (2, 1, 6, 8), (1, 2, 4, 5)}
    assert isinstance(lsa - lsb, LinkStreamDF)
    assert (lsa - lsb).size == 4.9

    assert lsa.issuperset(lsa & lsb)
    assert lsb.issuperset(lsa & lsb)
    assert (lsa | lsb).issuperset(lsa)
    assert (lsa | lsb).issuperset(lsb)
    assert lsa.issuperset(lsa - lsb)

    assert set(lsa.neighbors(1)) == {(2, 2, 5)}
    assert set(lsa.neighbors(1, 'in')) == {(2, 6, 8), (2, 1, 3)}
    assert set(lsa.neighbors(1, 'both')) == {(2, 6, 8), (2, 1, 5)}

    nsa = NodeSetS({1})
    nsb = NodeSetS({2})
    ts = TimeSetDF([(1, 4)])
    assert set(lsa.substream(nsa, nsb, ts)) == {(1, 2, 2, 4)}
    
    nsma = NodeStreamDF([(1, 1, 4)])
    assert set(lsa.neighborhood(nsma, direction='out')) == {(2, 2, 4)}

    nsmb = NodeStreamDF([(1, 1, 4), (2, 1, 2)])
    assert set(lsa.induced_substream(nsmb)) == {(1, 2, 2, 2), (2, 1, 1, 2)}


    try:
        lsa | 1
    except UnrecognizedLinkStream:
        pass

    try:
        lsa & 1
    except UnrecognizedLinkStream:
        pass

    try:
        lsa - 1
    except UnrecognizedLinkStream:
        pass

    df = [(1, 2, 2, 10), (2, 3, 4, 16), (1, 3, 6, 12), (3, 4, 8, 16), (2, 4, 13, 17)]
    assert LinkStreamDF(df).get_maximal_cliques() == {(frozenset({2, 4}), (13, 17)),
                                                      (frozenset({3, 4}), (8, 16)),
                                                      (frozenset({2, 3, 4}), (13, 16)),
                                                      (frozenset({2, 3}), (4, 16)),
                                                      (frozenset({1, 2, 3}), (6, 10)),
                                                      (frozenset({1, 3}), (6, 12)),
                                                      (frozenset({1, 2}), (2, 10))}

    

if __name__ == "__main__":
    test_link_stream_df()
