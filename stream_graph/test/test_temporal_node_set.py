"""Test file for time set."""
from itertools import product
from pandas import DataFrame
from stream_graph import NodeSetS
from stream_graph import TimeSetDF
from stream_graph import TemporalNodeSetB
from stream_graph import TemporalNodeSetDF
from stream_graph import ITemporalNodeSetDF
from stream_graph.collections import TimeGenerator
from stream_graph.collections import TimeCollection
from stream_graph.exceptions import UnrecognizedTimeSet
from stream_graph.exceptions import UnrecognizedTemporalNodeSet


def test_temporal_node_set_b():
    for d in [False, True]:
        nodeset = NodeSetS({1, 2, 3, 5})
        timeset = [(1, 2), (3, 5), (6, 7)]
        nsa = TemporalNodeSetB(nodeset, timeset, discrete=d)

        assert bool(nsa)
        assert not bool(TemporalNodeSetB([], [], discrete=d))
        assert not bool(TemporalNodeSetB([], [(1, 2)], discrete=d))
        assert not bool(TemporalNodeSetB([1], [], discrete=d))
        assert not bool(TemporalNodeSetB())

        assert set(nsa.nodeset) == set(nodeset)
        assert set(nsa.timeset) == set(timeset)
        assert nsa.timeset.discrete == d

        assert set(nsa) == {(n, ts, tf) for n in nodeset for ts, tf in timeset}

        assert nsa.n == 4
        assert nsa.size == (4 + len(timeset)*int(d))*nsa.n
        assert nsa.total_time == (4 + len(timeset)*int(d))
        assert TemporalNodeSetB().size == 0
        assert TemporalNodeSetB([], []).size == 0
        assert TemporalNodeSetB([], [(1, 2)]).size == 0
        assert TemporalNodeSetB([1], []).size == 0

        assert (2, None) in nsa
        assert (None, 3.4) in nsa
        assert (None, None) not in nsa
        assert (2, 3.4) in nsa
        assert (2, (3.4, 4)) in nsa

        assert nsa.node_duration(4) == 0
        assert nsa.node_duration(1) == 4 + len(timeset)*int(d)
        assert dict(nsa.node_duration()) == {i: nodeset.size + len(timeset)*int(d) for i in [1, 2, 3, 5]}

        assert nsa.common_time(1) == (nodeset.size-1)*(4 + len(timeset)*int(d))
        assert nsa.common_time_pair((2, 3)) == 4 + len(timeset)*int(d)
        assert nsa.common_time(7) == .0
        assert nsa.common_time_pair((2, 8)) == .0
        assert dict(nsa.common_time()) == {i: (nodeset.size-1)*(4 + len(timeset)*int(d)) for i in [1, 2, 3, 5]}
        assert dict(nsa.common_time_pair()) == {i: 4 + len(timeset)*int(d) for i in [(1, 2), (1, 3), (1, 5), (2, 3), (2, 5), (3, 5)]}

        assert set(nsa.nodes_at(1)) == {1, 2, 3, 5}
        assert set(nsa.nodes_at((1, 1.5))) == {1, 2, 3, 5}
        assert set(nsa.nodes_at(2.5)) == set()
        
        tg = nsa.nodes_at()
        assert isinstance(tg, TimeGenerator)
        assert [(t, set(ns)) for t, ns in tg] == [(1, {1, 2, 3, 5}), (2, set()), (3, {1, 2, 3, 5}), (5, set()), (6, {1, 2, 3, 5}), (7, set())]
        assert not tg.instants
        assert not len([a for a in tg])

        assert set(nsa.times_of(1)) == {(1, 2), (3, 5), (6, 7)}
        assert set(nsa.times_of(10)) == set()
        assert {i:list(t) for i, t in nsa.times_of()} == {1: [(1, 2), (3, 5), (6, 7)], 2: [(1, 2), (3, 5), (6, 7)], 3: [(1, 2), (3, 5), (6, 7)], 5: [(1, 2), (3, 5), (6, 7)]}

        assert nsa.n_at(6) == 4
        assert nsa.n_at(10) == 0
        assert list(nsa.n_at()) == [(1, 4), (2, 0), (3, 4), (5, 0), (6, 4), (7, 0)]

        nodeset = NodeSetS({2, 3, 4})
        timeset = [(1, 3), (4, 8)]
        nsb = TemporalNodeSetB(nodeset, timeset, discrete=d)
        assert set(nsb & nsa) == set(nsa & nsb)
        assert isinstance(nsa & nsb, TemporalNodeSetB)
        assert set(nsa & nsb) == {(n, ts, tf) for n in {2, 3} for ts, tf in [(1, 2), (3, 3), (4, 5), (6, 7)]}
        assert (nsb & nsa).size == (nsb & nsa).nodeset_.size*(3 + 4*int(d))
        assert (nsa & nsb).discrete == (nsb & nsa).discrete and (nsa & nsb).discrete == d

        assert set(nsb | nsa) == set(nsa | nsb)
        assert isinstance(nsa | nsb, TemporalNodeSetB)
        assert set((nsb | nsa)) == {(n, 1, 8) for n in {1, 2, 3, 4, 5}}
        assert (nsb | nsa).size == (nsb | nsa).nodeset_.size*(7 + int(d))
        assert (nsa | nsb).discrete == (nsb | nsa).discrete and (nsa | nsb).discrete == d

        diff_ba = set((n, ts, tf) for e in [([4], [(1, 3), (4, 8)]), ([2, 3], [(2, 3), (5, 6), (7, 8)])] for n in e[0] for ts, tf in e[1])
        assert set(nsb - nsa) == diff_ba
        assert (nsb - nsa).size == (12 + 8*int(d))
        diff_ab = set((n, ts, tf) for e in [([1, 5], [(1, 2), (3, 5), (6, 7)]), ([2, 3], [(3, 4)])] for n in e[0] for ts, tf in e[1])
        assert set(nsa - nsb) == diff_ab
        assert (nsa - nsb).size == (10 + (3*2 + 2*1)*int(d))
        assert (nsa - nsb).discrete == (nsb - nsa).discrete and (nsa - nsb).discrete == d

        assert nsa.issuperset(nsa & nsb)
        assert nsb.issuperset(nsa & nsb)
        assert (nsa | nsb).issuperset(nsa)
        assert (nsa | nsb).issuperset(nsb)
        assert nsa.issuperset(nsa - nsb)

        try:
            nsa | 1
        except UnrecognizedTemporalNodeSet:
            pass

        try:
            nsa & 1
        except UnrecognizedTemporalNodeSet:
            pass

        try:
            nsa - 1
        except UnrecognizedTemporalNodeSet:
            pass


def test_temporal_node_set_df():
    for d in [False, True]:
        df = [(1, 2, 3), (1, 3, 5), (1, 6, 8), (2, 1, 3)]
        nsa = TemporalNodeSetDF(df, disjoint_intervals=False, discrete=d)

        assert bool(nsa)
        assert not bool(TemporalNodeSetDF([]))
        assert not bool(TemporalNodeSetDF())

        assert set(nsa) == {(1, 2, 5), (1, 6, 8), (2, 1, 3)}

        assert nsa.n == 2
        assert nsa.size == 7 + 3*int(d)
        assert nsa.total_time == 6 + 2*int(d)
        assert TemporalNodeSetDF().size == 0
        assert TemporalNodeSetDF([]).size == 0
        assert nsa.discrete == d

        assert (2, None) in nsa
        assert (None, 3.4) in nsa
        assert (None, None) not in nsa
        assert (2, 3.4) not in nsa
        assert (2, (3.4, 4)) not in nsa

        assert nsa.node_duration(1) == 5 + 2*int(d)
        assert nsa.node_duration(4) == 0
        assert dict(nsa.node_duration()) == {1: 5.0 + 2*int(d), 2: 2.0 + int(d)}

        assert nsa.common_time(1) == 1 + int(d)
        assert nsa.common_time_pair((1, 2)) == 1 + int(d)
        assert nsa.common_time_pair((1, 2)) == nsa.common_time_pair((2, 1))
        assert nsa.common_time(7) == .0
        assert nsa.common_time_pair((2, 8)) == .0
        assert dict(nsa.common_time()) == {i: 1 + int(d) for i in range(1, 3)}
        assert dict(nsa.common_time_pair()) == {(1, 2): 1 + int(d)}
        assert dict(nsa.common_time_pair([(1, 2), (2, 1)])) == {i: 1 + int(d) for i in [(1, 2), (2, 1)]}

        assert set(nsa.nodes_at(1)) == {2}
        assert set(nsa.nodes_at((2, 2.5))) == {1, 2}
        assert set(nsa.nodes_at(10)) == set()

        tg = nsa.nodes_at()
        assert isinstance(tg, TimeGenerator)
        assert [(t, set(ns)) for t, ns in tg] == [(1, {2}), (2, {1, 2}), (3, {1}), (5, set()), (6, {1}), (8, set())]
        assert not tg.instants
        assert not len([a for a in tg])

        assert set(nsa.times_of(1)) == {(2, 5), (6, 8)}
        assert set(nsa.times_of(10)) == set()
        assert [(t, list(ts)) for t, ts in nsa.times_of()] == [(1, [(2, 5), (6, 8)]), (2, [(1, 3)])]

        assert nsa.n_at(2) == 2
        assert nsa.n_at(10) == 0
        
        tg = nsa.n_at()
        assert isinstance(tg, TimeCollection)
        assert list(tg) == [(1, 1), (2, 2), (3, 1), (5, 0), (6, 1), (8, 0)]
        assert not tg.instants
        assert len(list(tg))

        df = [(1, 1, 4), (1, 6, 7), (2, 2.5, 2.6)]
        nsb = TemporalNodeSetDF(df, discrete=d)
        assert set(nsb & nsa) == set(nsa & nsb)
        assert isinstance(nsa & nsb, TemporalNodeSetDF)
        assert set(nsa & nsb) == {(1, 2, 4), (1, 6, 7), (2, 2.5, 2.6)}
        assert (nsa & nsb).size == 3.1 + 3*int(d)
        assert (nsa & nsb).discrete == d

        assert set(nsb | nsa) == set(nsa | nsb)
        assert isinstance(nsa | nsb, TemporalNodeSetDF)
        assert set((nsb | nsa)) == {(1, 1, 5), (1, 6, 8), (2, 1, 3)}
        assert (nsb | nsa).size == 8 + 3*int(d)
        assert (nsb | nsa).discrete == d

        assert set(nsb - nsa) == {(1, 1, 2)}
        assert (nsb - nsa).size == 1 + int(d)
        assert isinstance(nsb - nsa, TemporalNodeSetDF)
        assert set(nsa - nsb) == {(1, 4, 5), (1, 7, 8), (2, 1, 2.5), (2, 2.6, 3)}
        assert isinstance(nsa - nsb, TemporalNodeSetDF)
        assert (nsa - nsb).size == 3.9 + 4*int(d)
        assert (nsa - nsb).discrete == d

        assert nsa.issuperset(nsa & nsb)
        assert nsb.issuperset(nsa & nsb)
        assert (nsa | nsb).issuperset(nsa)
        assert (nsa | nsb).issuperset(nsb)
        assert nsa.issuperset(nsa - nsb)

        try:
            nsa | 1
        except UnrecognizedTemporalNodeSet:
            pass

        try:
            nsa & 1
        except UnrecognizedTemporalNodeSet:
            pass

        try:
            nsa - 1
        except UnrecognizedTemporalNodeSet:
            pass

def test_itemporal_node_set_df():
    assert ITemporalNodeSetDF().size == 0
    assert ITemporalNodeSetDF([]).size == 0
    assert ITemporalNodeSetDF([]).discrete is None

    for d in [False, True]:
        df = [(1, 2), (1, 3), (1, 6), (2, 3), (2, 3)]
        nsa = ITemporalNodeSetDF(df, no_duplicates=False, discrete=d)

        assert bool(nsa)
        assert not bool(ITemporalNodeSetDF([]))
        assert not bool(ITemporalNodeSetDF())

        assert set(nsa) == {(1, 2), (1, 3), (1, 6), (2, 3)}

        assert nsa.n == 2
        assert nsa.size == int(d)*4
        assert nsa.total_time == 3*int(d)

        assert (2, None) in nsa
        assert (None, 5) not in nsa
        assert (None, 2) in nsa
        assert (2, 3) in nsa
        assert (1, 7) not in nsa

        assert nsa.node_duration(1) == int(d)*3
        assert nsa.node_duration(4) == 0
        assert dict(nsa.node_duration()) == {1: int(d)*3, 2: int(d)}

        assert nsa.common_time(1) == int(d)
        assert dict(nsa.common_time()) == {1: int(d), 2: int(d)}
        assert nsa.common_time_pair((1, 2)) == int(d)
        assert nsa.common_time_pair((2, 8)) == 0
        assert dict(nsa.common_time_pair()) == {(1, 2): int(d)}

        assert set(nsa.nodes_at(2)) == {1}
        assert set(nsa.nodes_at(3)) == {1, 2}
        assert set(nsa.nodes_at(5)) == set()
        
        tg = nsa.nodes_at()
        assert isinstance(tg, TimeGenerator)
        assert [(t, set(ns)) for t, ns in tg] == [(2, {1}), (3, {1, 2}), (6, {1})]
        assert tg.instants
        assert not len([a for a in tg])

        assert set(nsa.times_of(1)) == {2, 3, 6}
        assert set(nsa.times_of(10)) == set()
        assert [(n, set(ts)) for n, ts in nsa.times_of()] == [(1, {2, 3, 6}), (2, {3})]

        assert nsa.n_at(2) == 1
        assert nsa.n_at(10) == 0
        assert list(nsa.n_at()) == [(2, 1), (3, 2), (6, 1)]

        df = [(1, 2), (1, 3), (2, 4)]
        nsb = ITemporalNodeSetDF(df, discrete=d)
        assert set(nsb & nsa) == set(nsa & nsb)
        assert isinstance(nsa & nsb, ITemporalNodeSetDF)
        assert set(nsa & nsb) == {(1, 2), (1, 3)}
        assert (nsa & nsb).size == 2*int(d)
        assert (nsa & nsb).discrete == d

        assert set(nsb | nsa) == set(nsa | nsb)
        assert isinstance(nsa | nsb, ITemporalNodeSetDF)
        assert set((nsb | nsa)) == {(1, 2), (1, 3), (1, 6), (2, 3), (2, 4)}
        assert (nsa | nsb).size == 5*int(d)
        assert (nsa | nsb).discrete == d

        assert set(nsb - nsa) == {(2, 4)}
        assert (nsb - nsa).size == int(d)
        assert isinstance(nsb - nsa, ITemporalNodeSetDF)
        assert set(nsa - nsb) == {(1, 6), (2, 3)}
        assert (nsa - nsb).size == 2*int(d)
        assert isinstance(nsa - nsb, ITemporalNodeSetDF)
        assert (nsa - nsb).discrete == d
        assert (nsb - nsa).discrete == d

        assert nsa.issuperset(nsa & nsb)
        assert nsb.issuperset(nsa & nsb)
        assert (nsa | nsb).issuperset(nsa)
        assert (nsa | nsb).issuperset(nsb)
        assert nsa.issuperset(nsa - nsb)

        try:
            nsa | 1
        except UnrecognizedTemporalNodeSet:
            pass

        try:
            nsa & 1
        except UnrecognizedTemporalNodeSet:
            pass

        try:
            nsa - 1
        except UnrecognizedTemporalNodeSet:
            pass
    

def test_temporal_node_set_op_b_df():
    nsa = TemporalNodeSetB({1, 2}, [(1, 2), (4, 7)])
    nsb = TemporalNodeSetDF([(1, 2, 5), (1, 6, 8), (2, 1, 3)])
    assert set(nsb & nsa) == set(nsa & nsb)
    assert type(nsb & nsa) is type(nsa & nsb)
    assert isinstance(nsa & nsb, TemporalNodeSetDF)
    assert set(nsa & nsb) == {(1, 2, 2), (1, 6, 7), (2, 1, 2), (1, 4, 5)}
    assert (nsb & nsa).size == 3

    assert set(nsb | nsa) == set(nsa | nsb)
    assert type(nsb | nsa) is type(nsa | nsb)
    assert isinstance(nsa | nsb, TemporalNodeSetDF)
    assert set((nsb | nsa)) == {(1, 1, 8), (2, 1, 3), (2, 4, 7)}
    assert (nsb | nsa).size == 12

    assert set(nsb - nsa) == { (1, 2, 4), (1, 7, 8), (2, 2, 3)}
    assert (nsb - nsa).size == 4
    assert isinstance(nsb - nsa, TemporalNodeSetDF) and type(nsb - nsa) is type(nsa - nsb)
    assert set(nsa - nsb) == {(1, 1, 2), (1, 5, 6), (2, 4, 7)}
    assert (nsa - nsb).size == 5

    assert nsa.issuperset(nsa & nsb)
    assert nsb.issuperset(nsa & nsb)
    assert (nsa | nsb).issuperset(nsa)
    assert (nsa | nsb).issuperset(nsb)
    assert nsa.issuperset(nsa - nsb)


if __name__ == "__main__":
    test_temporal_node_set_b()
    test_temporal_node_set_df()
    test_itemporal_node_set_df()
    test_temporal_node_set_op_b_df()
