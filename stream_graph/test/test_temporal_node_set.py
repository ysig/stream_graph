"""Test file for time set."""
from stream_graph import NodeSetS
from stream_graph import TemporalNodeSetB
from stream_graph import TemporalNodeSetDF
from stream_graph import ITemporalNodeSetDF
from stream_graph.collections import TimeGenerator
from stream_graph.collections import TimeCollection
from stream_graph.exceptions import UnrecognizedTemporalNodeSet
from nose.tools import assert_equal


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

        assert_equal(set(nsa.nodeset), set(nodeset))
        if not d:
            timeset = [t + ('both',) for t in timeset]
        assert_equal(set(nsa.timeset), set(timeset))
        assert_equal(nsa.timeset.discrete, d)

        assert_equal(set(nsa), {(n,) + obj for n in nodeset for obj in timeset})

        assert_equal(nsa.n, 4)
        assert_equal(nsa.size, (4 + len(timeset)*int(d))*nsa.n)
        assert_equal(nsa.total_time, (4 + len(timeset)*int(d)))
        assert_equal(TemporalNodeSetB().size, 0)
        assert_equal(TemporalNodeSetB([], []).size, 0)
        assert_equal(TemporalNodeSetB([], [(1, 2)]).size, 0)
        assert_equal(TemporalNodeSetB([1], []).size, 0)

        assert (2, None) in nsa
        assert (None, 3.4) in nsa
        assert (None, None) not in nsa
        assert (2, 3.4) in nsa
        assert (2, (3.4, 4)) in nsa

        assert_equal(nsa.node_duration(4), 0)
        assert_equal(nsa.node_duration(1), 4 + len(timeset)*int(d))
        assert_equal(dict(nsa.node_duration()), {i: nodeset.size + len(timeset)*int(d) for i in [1, 2, 3, 5]})

        assert_equal(nsa.common_time(1), (nodeset.size-1)*(4 + len(timeset)*int(d)))
        assert_equal(nsa.common_time_pair((2, 3)), 4 + len(timeset)*int(d))
        assert_equal(nsa.common_time(7), .0)
        assert_equal(nsa.common_time_pair((2, 8)), .0)
        assert_equal(dict(nsa.common_time()), {i: (nodeset.size-1)*(4 + len(timeset)*int(d)) for i in [1, 2, 3, 5]})
        assert_equal(dict(nsa.common_time_pair()), {i: 4 + len(timeset)*int(d) for i in [(1, 2), (1, 3), (1, 5), (2, 3), (2, 5), (3, 5)]})

        assert_equal(set(nsa.nodes_at(1)), {1, 2, 3, 5})
        assert_equal(set(nsa.nodes_at((1, 1.5))), {1, 2, 3, 5})
        assert_equal(set(nsa.nodes_at(2.5)), set())

        tg = nsa.nodes_at()
        assert isinstance(tg, TimeGenerator)
        if d:
            assert_equal([(t, set(ns)) for t, ns in tg], [(1, {1, 2, 3, 5}), (3, set()), (3, {1, 2, 3, 5}), (6, set()), (6, {1, 2, 3, 5}), (8, set())])
        else:
            assert_equal([(t, set(ns)) for t, ns in tg], [((1, True), {1, 2, 3, 5}), ((2, False), set()), ((3, True), {1, 2, 3, 5}), ((5, False), set()), ((6, True), {1, 2, 3, 5}), ((7, False), set())])
        assert not tg.instants
        assert not len([a for a in tg])

        it = (tuple() if d else ('both', ))
        assert_equal(set(nsa.times_of(1)), {(1, 2) + it, (3, 5) + it, (6, 7) + it})
        assert_equal(set(nsa.times_of(10)), set())
        assert_equal({i:list(t) for i, t in nsa.times_of()}, {1: [(1, 2) + it, (3, 5) + it, (6, 7) + it], 2: [(1, 2) + it, (3, 5) + it, (6, 7) + it], 3: [(1, 2) + it, (3, 5) + it, (6, 7) + it], 5: [(1, 2) + it, (3, 5) + it, (6, 7) + it]})

        assert_equal(nsa.n_at(6), 4)
        assert_equal(nsa.n_at(10), 0)
        if d:
            assert_equal(list(nsa.n_at()), [(1, 4), (3, 0), (3, 4), (6, 0), (6, 4), (8, 0)])
        else:
            assert_equal(list(nsa.n_at()), [((1, True), 4), ((2, False), 0), ((3, True), 4), ((5, False), 0), ((6, True), 4), ((7, False), 0)])

        nodeset = NodeSetS({2, 3, 4})
        timeset = [(1, 3), (4, 8)]
        nsb = TemporalNodeSetB(nodeset, timeset, discrete=d)
        assert_equal(set(nsb & nsa), set(nsa & nsb))
        assert isinstance(nsa & nsb, TemporalNodeSetB)
        assert_equal(set(nsa & nsb), {(n, ) + t for n in {2, 3} for t in [(1, 2) + it, (3, 3) + it, (4, 5) + it, (6, 7) + it]})
        assert_equal((nsb & nsa).size, (nsb & nsa).nodeset_.size*(3 + 4*int(d)))
        assert_equal((nsa & nsb).discrete, (nsb & nsa).discrete and (nsa & nsb).discrete, d)

        assert_equal(set(nsb | nsa), set(nsa | nsb))
        assert isinstance(nsa | nsb, TemporalNodeSetB)
        assert_equal(set((nsb | nsa)), {(n, 1, 8) + it for n in {1, 2, 3, 4, 5}})
        assert_equal((nsb | nsa).size, (nsb | nsa).nodeset_.size*(7 + int(d)))
        assert (nsa | nsb).discrete == (nsb | nsa).discrete and (nsa | nsb).discrete == d

        if d:
            diff_ba = {(3, 8, 8), (4, 1, 3), (2, 8, 8), (4, 4, 8)}
        else:
            diff_ba = {(3, 2, 3, 'neither'), (4, 4, 8, 'both'), (2, 7, 8, 'right'), (2, 2, 3, 'neither'), (2, 5, 6, 'neither'), (4, 1, 3, 'both'), (3, 7, 8, 'right'), (3, 5, 6, 'neither')}
        assert_equal(set(nsb - nsa), diff_ba)
        assert_equal((nsb - nsa).size, (10 if d else 12))
        if d:
            diff_ab = {(5, 1, 2), (5, 6, 7), (1, 1, 2), (1, 3, 5), (1, 6, 7), (5, 3, 5)}
        else:
            diff_ab = {(5, 3, 5, 'both'), (1, 6, 7, 'both'), (3, 3, 4, 'neither'), (5, 6, 7, 'both'), (2, 3, 4, 'neither'), (1, 1, 2, 'both'), (5, 1, 2, 'both'), (1, 3, 5, 'both')}
        assert_equal(set(nsa - nsb), diff_ab)
        assert_equal((nsa - nsb).size, (14 if d else 10))
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
        print(d)
        df = [(1, 2, 3), (1, 3, 5), (1, 6, 8), (2, 1, 3)]
        nsa = TemporalNodeSetDF(df, disjoint_intervals=False, discrete=d)

        assert bool(nsa)
        assert not bool(TemporalNodeSetDF([]))
        assert not bool(TemporalNodeSetDF())

        if d:
            assert_equal(set(nsa), {(1, 2, 8), (2, 1, 3)})
        else:
            assert_equal(set(nsa), set(a + ('both',) for a in [(1, 2, 5), (1, 6, 8), (2, 1, 3)]))

        assert_equal(nsa.n, 2)
        assert_equal(nsa.size, 7 + 3*int(d))
        assert_equal(nsa.total_time, 6 + 2*int(d))
        assert_equal(TemporalNodeSetDF().size, 0)
        assert_equal(TemporalNodeSetDF([]).size, 0)
        assert_equal(nsa.discrete, d)

        assert (2, None) in nsa
        assert (None, 3.4) in nsa
        assert (None, None) not in nsa
        assert (2, 3.4) not in nsa
        assert (2, (3.4, 4)) not in nsa

        assert_equal(nsa.node_duration(1), 5 + 2*int(d))
        assert_equal(nsa.node_duration(4), 0)
        assert_equal(dict(nsa.node_duration()), {1: 5.0 + 2*int(d), 2: 2.0 + int(d)})

        assert_equal(nsa.common_time(1), 1 + int(d))
        assert_equal(nsa.common_time_pair((1, 2)), 1 + int(d))
        assert_equal(nsa.common_time_pair((1, 2)), nsa.common_time_pair((2, 1)))
        assert_equal(nsa.common_time(7), .0)
        assert_equal(nsa.common_time_pair((2, 8)), .0)
        assert_equal(dict(nsa.common_time()), {i: 1 + int(d) for i in range(1, 3)})
        assert_equal(dict(nsa.common_time_pair()), {(1, 2): 1 + int(d)})
        assert_equal(dict(nsa.common_time_pair([(1, 2), (2, 1)])), {i: 1 + int(d) for i in [(1, 2), (2, 1)]})

        assert_equal(set(nsa.nodes_at(1)), {2})
        assert_equal(set(nsa.nodes_at((2, 2.5))), {1, 2})
        assert_equal(set(nsa.nodes_at(10)), set())

        tg = nsa.nodes_at()
        assert isinstance(tg, TimeGenerator)
        if d:
            assert_equal([(t, set(ns)) for t, ns in tg], [(3, {2}), (4, set()), (5, {1}), (6, set())])
        else:
            assert_equal([(t, set(ns)) for t, ns in tg], [((1, True), {2}), ((2, False), {1, 2}), ((3, False), {1}), ((5, False), set()), ((6, False), {1}), ((8, False), set())])

        assert not tg.instants
        assert not len([a for a in tg])

        assert_equal(set(nsa.times_of(1)), ({(2, 8)} if d else {(2, 5, 'both'), (6, 8, 'both')}))
        assert_equal(set(nsa.times_of(10)), set())
        if d:
            assert_equal([(t, list(ts)) for t, ts in nsa.times_of()], [(2, [(1, 3)]), (1, [(2, 8)])])
        else:
            assert_equal([(t, list(ts)) for t, ts in nsa.times_of()], [(1, [(2, 5, 'both'), (6, 8, 'both')]), (2, [(1, 3, 'both')])])

        assert_equal(nsa.n_at(2), 2)
        assert_equal(nsa.n_at(10), 0)

        tg = nsa.n_at()
        assert isinstance(tg, TimeCollection)
        if d:
            assert_equal(list(tg), [(3, 1), (4, 0), (5, 1), (6, 0)])
        else:
            assert_equal(list(tg), [((1, True), 1), ((2, False), 2), ((3, False), 1), ((5, False), 0), ((6, False), 1), ((8, False), 0)])
        assert not tg.instants
        assert len(list(tg))

        if d:
            df = [(1, 1, 3), (1, 6, 7), (2, 2, 2)]
        else:
            df = [(1, 1, 4), (1, 6, 7), (2, 2.5, 2.6)]
        nsb = TemporalNodeSetDF(df, discrete=d)
        assert_equal(set(nsb & nsa), set(nsa & nsb))
        assert isinstance(nsa & nsb, TemporalNodeSetDF)
        if d:
            assert_equal(set(nsa & nsb), {(1, 2, 3), (1, 6, 7), (2, 2, 2)})
        else:
            assert_equal(set(nsa & nsb), {(1, 2, 4, 'both'), (1, 6, 7, 'both'), (2, 2.5, 2.6, 'both')})
        assert_equal((nsa & nsb).size, (5 if d else 3.1))
        assert_equal((nsa & nsb).discrete, d)

        assert_equal(set(nsb | nsa), set(nsa | nsb))
        assert isinstance(nsa | nsb, TemporalNodeSetDF)
        if d:
            assert_equal(set((nsb | nsa)), {(1, 1, 3), (1, 6, 8), (2, 1, 3)})
        else:
            assert_equal(set((nsb | nsa)), {(1, 1, 5, 'both'), (1, 6, 8, 'both'), (2, 1, 3, 'both')})
        assert_equal((nsb | nsa).size, 8 + int(d))
        assert_equal((nsb | nsa).discrete, d)

        if d:
            assert_equal(set(nsb - nsa), {(1, 1, 1)})
        else:
            assert_equal(set(nsb - nsa), {(1, 1, 2, 'left')})
        assert_equal((nsb - nsa).size, 1)
        assert isinstance(nsb - nsa, TemporalNodeSetDF)
        if d:
            assert_equal(set(nsa - nsb), {(1, 4, 5), (1, 8, 8), (2, 1, 1), (2, 3, 3)})
        else:
            assert_equal(set(nsa - nsb), {(2, 2.6, 3.0, 'right'), (2, 1.0, 2.5, 'left'), (1, 7.0, 8.0, 'right'), (1, 4.0, 5.0, 'right')})
        assert isinstance(nsa - nsb, TemporalNodeSetDF)
        assert_equal((nsa - nsb).size, (5 if d else 3.9))
        assert_equal((nsa - nsb).discrete, d)

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
    assert_equal(ITemporalNodeSetDF().size, 0)
    assert_equal(ITemporalNodeSetDF([]).size, 0)
    assert ITemporalNodeSetDF([]).discrete is True

    for d in [False, True]:
        df = [(1, 2), (1, 3), (1, 6), (2, 3), (2, 3)]
        nsa = ITemporalNodeSetDF(df, no_duplicates=False, discrete=d)

        assert bool(nsa)
        assert not bool(ITemporalNodeSetDF([]))
        assert not bool(ITemporalNodeSetDF())

        assert_equal(set(nsa), {(1, 2), (1, 3), (1, 6), (2, 3)})

        assert_equal(nsa.n, 2)
        assert_equal(nsa.size, int(d)*4)
        assert_equal(nsa.total_time, 3*int(d))

        assert (2, None) in nsa
        assert (None, 5) not in nsa
        assert (None, 2) in nsa
        assert (2, 3) in nsa
        assert (1, 7) not in nsa

        assert_equal(nsa.node_duration(1), int(d)*3)
        assert_equal(nsa.node_duration(4), 0)
        assert_equal(dict(nsa.node_duration()), {1: int(d)*3, 2: int(d)})

        assert_equal(nsa.common_time(1), int(d))
        assert_equal(dict(nsa.common_time()), {1: int(d), 2: int(d)})
        assert_equal(nsa.common_time_pair((1, 2)), int(d))
        assert_equal(nsa.common_time_pair((2, 8)), 0)
        assert_equal(dict(nsa.common_time_pair()), {(1, 2): int(d)})

        assert_equal(set(nsa.nodes_at(2)), {1})
        assert_equal(set(nsa.nodes_at(3)), {1, 2})
        assert_equal(set(nsa.nodes_at(5)), set())

        tg = nsa.nodes_at()
        assert isinstance(tg, TimeGenerator)
        assert_equal([(t, set(ns)) for t, ns in tg], [(2, {1}), (3, {1, 2}), (6, {1})])
        assert tg.instants
        assert not len([a for a in tg])

        assert_equal(set(nsa.times_of(1)), {2, 3, 6})
        assert_equal(set(nsa.times_of(10)), set())
        assert_equal([(n, set(ts)) for n, ts in nsa.times_of()], [(1, {2, 3, 6}), (2, {3})])

        assert_equal(nsa.n_at(2), 1)
        assert_equal(nsa.n_at(10), 0)
        assert_equal(list(nsa.n_at()), [(2, 1), (3, 2), (6, 1)])

        df = [(1, 2), (1, 3), (2, 4)]
        nsb = ITemporalNodeSetDF(df, discrete=d)
        assert_equal(set(nsb & nsa), set(nsa & nsb))
        assert isinstance(nsa & nsb, ITemporalNodeSetDF)
        assert_equal(set(nsa & nsb), {(1, 2), (1, 3)})
        assert_equal((nsa & nsb).size, 2*int(d))
        assert_equal((nsa & nsb).discrete, d)

        assert_equal(set(nsb | nsa), set(nsa | nsb))
        assert isinstance(nsa | nsb, ITemporalNodeSetDF)
        assert_equal(set((nsb | nsa)), {(1, 2), (1, 3), (1, 6), (2, 3), (2, 4)})
        assert_equal((nsa | nsb).size, 5*int(d))
        assert_equal((nsa | nsb).discrete, d)

        assert_equal(set(nsb - nsa), {(2, 4)})
        assert_equal((nsb - nsa).size, int(d))
        assert isinstance(nsb - nsa, ITemporalNodeSetDF)
        assert_equal(set(nsa - nsb), {(1, 6), (2, 3)})
        assert_equal((nsa - nsb).size, 2*int(d))
        assert isinstance(nsa - nsb, ITemporalNodeSetDF)
        assert_equal((nsa - nsb).discrete, d)
        assert_equal((nsb - nsa).discrete, d)

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
    for d in [False, True]:
        nsa = TemporalNodeSetB({1, 2}, [(1, 2), (4, 7)], discrete=d)
        nsb = TemporalNodeSetDF([(1, 2, 5), (1, 6, 8), (2, 1, 3)], discrete=d)
        assert_equal(set(nsb & nsa), set(nsa & nsb))
        assert type(nsb & nsa) is type(nsa & nsb)
        assert isinstance(nsa & nsb, TemporalNodeSetDF)
        if d:
            assert_equal(set(nsa & nsb), {(1, 2, 2), (1, 6, 7), (2, 1, 2), (1, 4, 5)})
        else:
            assert_equal(set(nsa & nsb), {(1, 2, 2, 'both'), (1, 4, 5, 'both'), (2, 1, 2, 'both'), (1, 6, 7, 'both')})
        assert_equal((nsb & nsa).size, (7 if d else 3))

        assert_equal(set(nsb | nsa), set(nsa | nsb))
        assert type(nsb | nsa) is type(nsa | nsb)
        assert isinstance(nsa | nsb, TemporalNodeSetDF)
        if d:
            assert_equal(set((nsb | nsa)), {(1, 1, 8), (2, 1, 7)})
        else:
            assert_equal(set((nsb | nsa)), {(2, 4, 7, 'both'), (1, 1, 8, 'both'), (2, 1, 3, 'both')})
        assert_equal((nsb | nsa).size, (15 if d else 12))

        if d:
            assert_equal(set(nsb - nsa), {(1, 8, 8), (2, 3, 3), (1, 3, 3)})
        else:
            assert_equal(set(nsb - nsa), {(2, 2, 3, 'right'), (1, 2, 4, 'neither'), (1, 7, 8, 'right')})
        assert_equal((nsb - nsa).size, (3 if d else 4))
        if d:
            assert_equal(set(nsa - nsb), {(1, 1, 1), (2, 4, 7)})
        else:
            assert_equal(set(nsa - nsb), {(1, 5, 6, 'neither'), (1, 1, 2, 'left'), (2, 4, 7, 'both')})
        assert isinstance(nsb - nsa, TemporalNodeSetDF) and type(nsb - nsa) is type(nsa - nsb)
        assert_equal((nsa - nsb).size, 5)

        assert nsa.issuperset(nsa & nsb)
        assert nsb.issuperset(nsa & nsb)
        assert (nsa | nsb).issuperset(nsa)
        assert (nsa | nsb).issuperset(nsb)
        assert nsa.issuperset(nsa - nsb)


if __name__ == "__main__":
    test_temporal_node_set_df()
    test_itemporal_node_set_df()
    test_temporal_node_set_b()
    test_temporal_node_set_op_b_df()
