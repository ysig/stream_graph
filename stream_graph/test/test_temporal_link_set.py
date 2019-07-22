"""Test file for time set."""
from itertools import product
from pandas import DataFrame
from stream_graph import NodeSetS
from stream_graph import ITimeSetS
from stream_graph import TimeSetDF
from stream_graph import TemporalNodeSetB
from stream_graph import TemporalNodeSetDF
from stream_graph import ITemporalNodeSetDF
from stream_graph import TemporalLinkSetDF
from stream_graph import ITemporalLinkSetDF
from stream_graph.collections import TimeCollection
from stream_graph.collections import TimeGenerator
from stream_graph.exceptions import UnrecognizedTemporalLinkSet
from nose.tools import assert_equal


def test_temporal_link_set_df():
    assert not bool(TemporalLinkSetDF([]))
    assert not bool(TemporalLinkSetDF())

    assert_equal(TemporalLinkSetDF().size, 0)
    assert_equal(TemporalLinkSetDF([]).size, 0)

    assert TemporalLinkSetDF().discrete

    for d in [False, True]:
        df = [(1, 2, 2, 3), (1, 2, 3, 5), (2, 1, 6, 8), (2, 1, 1, 3)]
        lsa = TemporalLinkSetDF(df, disjoint_intervals=False, discrete=d)

        assert bool(lsa)
        if d:
            assert_equal(set(lsa), {(1, 2, 2, 5), (2, 1, 6, 8), (2, 1, 1, 3)})
        else:
            assert_equal(set(lsa), {(1, 2, 2, 5, 'both'), (2, 1, 6, 8, 'both'), (2, 1, 1, 3, 'both')})

        assert_equal(lsa.m, 2)
        assert_equal(lsa.size, 7 + 3*int(d))

        assert (1, 2, None) in lsa
        assert (1, None, 3.4) in lsa
        assert (5, None, None) not in lsa
        assert (1, 3, 3.4) not in lsa
        assert (1, 2, (3.4, 4)) in lsa

        assert_equal(lsa.duration_of((1, 2)), 3 + int(d))
        assert_equal(lsa.duration_of((2, 1)), 4 + 2*int(d))
        assert_equal(lsa.duration_of((2, 1)), lsa.duration_of((1, 2), direction='in'))
        assert_equal(lsa.duration_of((1, 2), direction='both'), 6 + 2*int(d))
        assert_equal(lsa.duration_of((5, 1)), 0)
        assert_equal(dict(lsa.duration_of()), {(1, 2): 3.0 + int(d), (2, 1): 4.0 + 2*int(d)})

        assert_equal(set(lsa.links_at(1)), {(2, 1)})
        assert_equal(set(lsa.links_at((2, 2.5))), {(1, 2), (2, 1)})
        assert_equal(set(lsa.links_at(10)), set())

        tg = lsa.links_at()
        assert isinstance(tg, TimeGenerator)
        if d:
            assert_equal([(t, set(ns)) for t, ns in tg], [(1, {(2, 1)}), (2, {(1, 2), (2, 1)}), (4, {(1, 2)}), (6, {(2, 1)}), (9, set())])
        else:
            assert_equal([(t, set(ns)) for t, ns in tg], [((1, True), {(2, 1)}), ((2, True), {(1, 2), (2, 1)}), ((3, False), {(1, 2)}), ((5, False), set()), ((6, True), {(2, 1)}), ((8, False), set())])
        assert not tg.instants
        assert not len([a for a in tg])

        if d:
            assert_equal(set(lsa.times_of((1, 2))), {(2, 5)})
            assert_equal({t: list(ns) for t, ns in lsa.times_of()}, {(1, 2): [(2, 5)], (2, 1): [(1, 3), (6, 8)]})
        else:
            assert_equal(set(lsa.times_of((1, 2))), {(2, 5, 'both')})
            assert_equal({t: list(ns) for t, ns in lsa.times_of()}, {(1, 2): [(2, 5, 'both')], (2, 1): [(1, 3, 'both'), (6, 8, 'both')]})
        assert_equal(set(lsa.times_of((10, 3))), set())

        assert_equal(lsa.m_at(2), 2)
        assert_equal(lsa.m_at(10), 0)

        df = [(1, 2, 1, 4), (1, 2, 6, 7), ((2, 1, 3, 5) if d else (2, 1, 2.5, 2.6))]
        lsb = TemporalLinkSetDF(df, discrete=d)
        assert_equal(set(lsb & lsa), set(lsa & lsb))
        assert isinstance(lsa & lsb, TemporalLinkSetDF)
        if d:
            assert_equal(set(lsa & lsb), {(1, 2, 2, 4), (2, 1, 3, 3)})
        else:
            assert_equal(set(lsa & lsb), {(2, 1, 2.5, 2.6, 'both'), (1, 2, 2.0, 4.0, 'both')})
        assert_equal((lsa & lsb).size, (4 if d else 2.1))
        # incorrect for the discrete case as the intervals are not discrete

        assert isinstance(lsa | lsb, TemporalLinkSetDF)
        if d:
            assert_equal(set((lsb | lsa)), {(2, 1, 1, 8), (1, 2, 1, 7)})
        else:
            assert_equal(set((lsb | lsa)), {(2, 1, 1, 3, 'both'), (2, 1, 6, 8, 'both'), (1, 2, 1, 5, 'both'), (1, 2, 6, 7, 'both')})
        assert_equal((lsb | lsa).size, 9 + 6*int(d))
        assert_equal(set(lsb | lsa), set(lsa | lsb))

        if d:
            assert_equal(set(lsb - lsa), {(2, 1, 4, 5), (1, 2, 1, 1), (1, 2, 6, 7)})
        else:
            assert_equal(set(lsb - lsa), {(1, 2, 1, 2, 'left'), (1, 2, 6, 7, 'both')})
        assert_equal((lsb - lsa).size, 2 + 3*int(d))
        assert isinstance(lsb - lsa, TemporalLinkSetDF)
        if d:
            assert_equal(set(lsa - lsb), {(2, 1, 1, 2), (2, 1, 6, 8), (1, 2, 5, 5)})
        else:
            assert_equal(set(lsa - lsb), {(1, 2, 4.0, 5.0, 'right'), (2, 1, 1.0, 2.5, 'left'), (2, 1, 6.0, 8.0, 'both'), (2, 1, 2.6, 3.0, 'right')})
        assert isinstance(lsa - lsb, TemporalLinkSetDF)
        assert_equal((lsa - lsb).size, 4.9 + 1.1*int(d))

        assert lsa.issuperset(lsa & lsb)
        assert lsb.issuperset(lsa & lsb)
        assert (lsa | lsb).issuperset(lsa)
        assert (lsa | lsb).issuperset(lsb)
        assert lsa.issuperset(lsa - lsb)

        if d:
            assert_equal(set(lsa.neighbors_of(1)), {(2, 2, 5)})
            assert_equal(set(lsa.neighbors_of(1, 'in')), {(2, 6, 8), (2, 1, 3)})
            assert_equal(set(lsa.neighbors_of(1, 'both')), {(2, 1, 8)})
            assert_equal({n: list(ns) for n, ns in lsa.neighbors_of(direction='out')}, {1: [(2, 2, 5)], 2: [(1, 1, 3), (1, 6, 8)]})
            assert_equal({n: list(ns) for n, ns in lsa.neighbors_of(direction='in')}, {1: [(2, 1, 3), (2, 6, 8)], 2: [(1, 2, 5)]})
            assert_equal({n: list(ns) for n, ns in lsa.neighbors_of(direction='both')}, {1: [(2, 1, 8)], 2: [(1, 1, 8)]})
        else:
            assert_equal(set(lsa.neighbors_of(1)), {(2, 2, 5, 'both')})
            assert_equal(set(lsa.neighbors_of(1, 'in')), {(2, 6, 8, 'both'), (2, 1, 3, 'both')})
            assert_equal(set(lsa.neighbors_of(1, 'both')), {(2, 6, 8, 'both'), (2, 1, 5, 'both')})
            assert_equal({n: list(ns) for n, ns in lsa.neighbors_of(direction='out')}, {1: [(2, 2, 5, 'both')], 2: [(1, 1, 3, 'both'), (1, 6, 8, 'both')]})
            assert_equal({n: list(ns) for n, ns in lsa.neighbors_of(direction='in')}, {2: [(1, 2, 5, 'both')], 1: [(2, 1, 3, 'both'), (2, 6, 8, 'both')]})
            assert_equal({n: list(ns) for n, ns in lsa.neighbors_of(direction='both')}, {2: [(1, 1, 5, 'both'), (1, 6, 8, 'both')], 1: [(2, 1, 5, 'both'), (2, 6, 8, 'both')]})

        na = lsa.neighbors_at(1)
        assert isinstance(na, TimeGenerator)
        if d:
            assert_equal([(t, set(ns)) for t, ns in na], [(2, {2}), (6, set())])
        else:
            assert_equal([(t, set(ns)) for t, ns in na], [((2, True), {2}), ((5, False), set())])
        assert not tg.instants
        assert not len([a for a in na])

        assert_equal(set(lsa.neighbors_at(1, 2)), {2})
        assert_equal({i: set(nsa) for i, nsa in lsa.neighbors_at(None, 2, 'out')}, {1: {2}, 2: {1}})
        if d:
            assert_equal({i: [(t, set(ts)) for t, ts in nsa] for i, nsa in lsa.neighbors_at()}, {1: [(2, {2}), (6, set())], 2: [(1, {1}), (4, set()), (6, {1}), (9, set())]})
        else:
            assert_equal({i: [(t, set(ts)) for t, ts in nsa] for i, nsa in lsa.neighbors_at()}, {1: [((2, True), {2}), ((5, False), set())], 2: [((1, True), {1}), ((3, False), set()), ((6, True), {1}), ((8, False), set())]})

        na = lsa.degree_at(1)
        assert isinstance(na, TimeCollection)
        if d:
            assert_equal([(t, ns) for t, ns in na], [(2, 1), (6, 0)])
        else:
            assert_equal([(t, ns) for t, ns in na], [((2, True), 1), ((5, False), 0)])
        assert not na.instants
        assert len([a for a in na])

        assert_equal(lsa.degree_at(1, 2), 1)
        assert_equal({i: n for i, n in lsa.degree_at(None, 2, 'out')}, {1: 1, 2: 1})

        if d:
            assert_equal({i: [(t, ts) for t, ts in nsa] for i, nsa in lsa.degree_at()}, {1: [(2, 1), (6, 0)], 2: [(1, 1), (4, 0), (6, 1), (9, 0)]})
        else:
            assert_equal({i: [(t, ts) for t, ts in nsa] for i, nsa in lsa.degree_at()}, {2: [((1, True), 1), ((3, False), 0), ((6, True), 1), ((8, False), 0)], 1: [((2, True), 1), ((5, False), 0)]})

        nsa = NodeSetS({1})
        nsb = NodeSetS({2})
        ts = TimeSetDF([(1, 4)], discrete=d)
        if d:
            assert_equal(set(lsa.substream(nsa, nsb, ts)), {(1, 2, 2, 4)})
        else:
            assert_equal(set(lsa.substream(nsa, nsb, ts)), {(1, 2, 2, 4, 'both')})

        nsmb = TemporalNodeSetDF([(1, 1, 4), (2, 1, 3)], discrete=d)
        assert_equal(set(lsa.induced_substream(nsmb)), {a + (tuple() if d else ('both', )) for a in [(1, 2, 2, 3), (2, 1, 1, 3)]})

        nsma = TemporalNodeSetDF([(1, 1, 4)], discrete=d)
        assert_equal(set(lsa.temporal_neighborhood(nsma, direction='out')), {(2, 2, 4)} if d else {(2, 2, 4, 'both')})


        try:
            lsa | 1
        except UnrecognizedTemporalLinkSet:
            pass

        try:
            lsa & 1
        except UnrecognizedTemporalLinkSet:
            pass

        try:
            lsa - 1
        except UnrecognizedTemporalLinkSet:
            pass

        df = [(1, 2, 2, 10), (2, 3, 4, 16), (1, 3, 6, 12), (3, 4, 8, 16), (2, 4, 13, 17)]
        assert_equal(TemporalLinkSetDF(df, discrete=d).get_maximal_cliques(),
                     {(frozenset({2, 4}), (13, 17)),
                      (frozenset({3, 4}), (8, 16)),
                      (frozenset({2, 3, 4}), (13, 16)),
                      (frozenset({2, 3}), (4, 16)),
                      (frozenset({1, 2, 3}), (6, 10)),
                      (frozenset({1, 3}), (6, 12)),
                      (frozenset({1, 2}), (2, 10))})


        # weighted
        df = [(1, 2, 2, 3, 1), (1, 2, 3, 5, 1), (2, 1, 6, 8, 1), (2, 1, 1, 3, 1)]
        lsa = TemporalLinkSetDF(df, disjoint_intervals=False, weighted=True, discrete=d)

        assert bool(lsa)
        if d:
            assert_equal(set(lsa), {(1, 2, 4, 5, 1), (2, 1, 1, 3, 1), (2, 1, 6, 8, 1), (1, 2, 3, 3, 2), (1, 2, 2, 2, 1)})
        else:
            assert_equal(set(lsa), {(2, 1, 6, 8, 'left', 1), (2, 1, 1, 3, 'left', 1), (1, 2, 2, 5, 'left', 1)})

        assert_equal(lsa.m, 2)
        assert_equal(lsa.size, 7 + 3*int(d))


        assert (1, 2, None) in lsa
        assert (5, None, None) not in lsa
        if d:
            assert (1, None, 3) in lsa
            assert (1, 3, 3) not in lsa
            #assert (1, 2, (2, 3)) in lsa
        else:
            assert (1, None, 3.4) in lsa
            assert (1, 3, 3.4) not in lsa
            assert (1, 2, (3.4, 4)) in lsa

        assert_equal(lsa.duration_of((1, 2)), 3 + int(d))
        assert_equal(lsa.duration_of((2, 1)), 4 + 2*int(d))
        assert_equal(lsa.duration_of((2, 1)), lsa.duration_of((1, 2), direction='in'))
        assert_equal(lsa.duration_of((1, 2), direction='both'), 6 + 2*int(d))
        assert_equal(lsa.duration_of((5, 1)), 0)
        assert_equal(dict(lsa.duration_of()), {(1, 2): 3 + int(d), (2, 1): 4.0 + 2*int(d)})

        assert_equal(set(lsa.links_at(1)), {(2, 1, 1)})
        if d:
            print('TODO: fix df_at and index_at_interval for weighted cases')
            pass
        else:
            assert_equal(set(lsa.links_at((2, 2.5))), {(1, 2, 1), (2, 1, 1)})
        assert_equal(set(lsa.links_at(10)), set())

        tg = lsa.links_at()
        assert isinstance(tg, TimeGenerator)
        if d:
            assert_equal([(t, set(ns)) for t, ns in tg], [(1, {(2, 1, 1)}), (2, {(1, 2, 1), (2, 1, 1)}), (3, {(1, 2, 2), (2, 1, 1)}), (4, {(1, 2, 1)}), (6, {(2, 1, 1)}), (9, set())])
        else:
            assert_equal([(t, set(ns)) for t, ns in tg], [((1, True), {(2, 1, 1)}), ((2, True), {(1, 2, 1), (2, 1, 1)}), ((3, True), {(1, 2, 1)}), ((5, True), set()), ((6, True), {(2, 1, 1)}), ((8, True), set())])
        assert not tg.instants
        assert not len([a for a in tg])

        if d:
            assert_equal(set(lsa.times_of((1, 2))), {(2, 5)})
            assert_equal({t: list(ns) for t, ns in lsa.times_of()}, {(1, 2): [(2, 5)], (2, 1): [(1, 3), (6, 8)]})
        else:
            assert_equal(set(lsa.times_of((1, 2))), {(2, 5, 'left')})
            assert_equal({t: list(ns) for t, ns in lsa.times_of()}, {(1, 2): [(2, 5, 'left')], (2, 1): [(1, 3, 'left'), (6, 8, 'left')]})
        assert_equal(set(lsa.times_of((10, 3))), set())

        assert_equal(lsa.m_at(2), 2)
        assert_equal(lsa.m_at(10), 0)

        df = ([(1, 2, 1, 4, 1), (1, 2, 6, 7, 1), (2, 1, 2, 3, 1)] if d else [(1, 2, 1, 4, 1), (1, 2, 6, 7, 1), (2, 1, 2.5, 2.6, 1)])
        lsb = TemporalLinkSetDF(df, weighted=True, discrete=d)
        assert_equal(set(lsb & lsa), set(lsa & lsb))
        assert isinstance(lsa & lsb, TemporalLinkSetDF)
        if d:
            assert_equal(set(lsa & lsb), {(2, 1, 2, 3, 1), (1, 2, 2, 4, 1)})
        else:
            assert_equal(set(lsa & lsb), {(2, 1, 2.5, 2.6, 'left', 1), (1, 2, 2.0, 4.0, 'left', 1)})
        assert_equal((lsa & lsb).size, 2.1 + 2.9*int(d))

        assert isinstance(lsa | lsb, TemporalLinkSetDF)
        if d:
            assert_equal(set((lsb | lsa)), {(2, 1, 1, 1, 1), (1, 2, 2, 2, 2), (1, 2, 5, 7, 1), (2, 1, 2, 3, 2), (1, 2, 3, 4, 3), (2, 1, 4, 5, 1), (1, 2, 1, 1, 1), (2, 1, 6, 8, 2)})
        else:
            assert_equal(set((lsb | lsa)), {(1, 2, 4.0, 5.0, 'left', 1), (2, 1, 2.6, 3.0, 'left', 1), (2, 1, 6.0, 8.0, 'left', 1), (1, 2, 2.0, 4.0, 'left', 2), (2, 1, 2.5, 2.6, 'left', 2), (1, 2, 6.0, 7.0, 'left', 1), (1, 2, 1.0, 2.0, 'left', 1), (2, 1, 1.0, 2.5, 'left', 1)})
        assert_equal((lsb | lsa).size, 6*int(d) + 9)
        assert_equal(set(lsb | lsa), set(lsa | lsb))

        if d:
            assert_equal(set(lsb - lsa), {(1, 2, 6, 7, 1), (1, 2, 1, 1, 1)})
        else:
            assert_equal(set(lsb - lsa), {(1, 2, 6.0, 7.0, 'left', 1.0), (1, 2, 1.0, 2.0, 'left', 1.0)})
        assert_equal((lsb - lsa).size, 2 + int(d))
        assert isinstance(lsb - lsa, TemporalLinkSetDF)
        if d:
            assert_equal(set(lsa - lsb), {(2, 1, 1, 1, 1), (2, 1, 6, 8, 1), (1, 2, 5, 5, 1), (1, 2, 3, 3, 1)})
        else:
            assert_equal(set(lsa - lsb), {(1, 2, 4.0, 5.0, 'left', 1.0), (2, 1, 1.0, 2.5, 'left', 1.0), (2, 1, 6.0, 8.0, 'left', 1.0), (2, 1, 2.6, 3.0, 'left', 1.0)})
        assert isinstance(lsa - lsb, TemporalLinkSetDF)
        assert_equal((lsa - lsb).size, 4.9 + 1.1*int(d))

        assert lsa.issuperset(lsa & lsb)
        assert lsb.issuperset(lsa & lsb)
        assert (lsa | lsb).issuperset(lsa)
        assert (lsa | lsb).issuperset(lsb)
        assert lsa.issuperset(lsa - lsb)

        if d:
            assert_equal(set(lsa.neighbors_of(1)), {(2, 2, 5)})
            assert_equal(set(lsa.neighbors_of(1, direction='in')), {(2, 6, 8), (2, 1, 3)})
            assert_equal(set(lsa.neighbors_of(1, direction='both')), {(2, 1, 8)})
            assert_equal({n: list(ns) for n, ns in lsa.neighbors_of()}, {1: [(2, 2, 5)], 2: [(1, 1, 3), (1, 6, 8)]})
        else:
            assert_equal(set(lsa.neighbors_of(1)), {(2, 2, 5, 'left')})
            assert_equal(set(lsa.neighbors_of(1, direction='in')), {(2, 6, 8, 'left'), (2, 1, 3, 'left')})
            assert_equal(set(lsa.neighbors_of(1, direction='both')), {(2, 6, 8, 'left'), (2, 1, 5, 'left')})
            assert_equal({n: list(ns) for n, ns in lsa.neighbors_of()}, {1: [(2, 2, 5, 'left')], 2: [(1, 1, 3, 'left'), (1, 6, 8, 'left')]})

        na = lsa.neighbors_at(1)
        assert isinstance(na, TimeGenerator)
        if d:
            assert_equal([(t, set(ns)) for t, ns in na], [(2, {2}), (6, set())])
        else:
            assert_equal([(t, set(ns)) for t, ns in na], [((2, True), {2}), ((5,  True), set())])
        assert not tg.instants
        assert not len([a for a in na])

        assert_equal(set(lsa.neighbors_at(1, 2)), {2})
        assert_equal({i: set(nsa) for i, nsa in lsa.neighbors_at(None, 2, 'out')}, {1: {2}, 2: {1}})
        if d:
            assert_equal({i: [(t, set(ts)) for t, ts in nsa] for i, nsa in lsa.neighbors_at()}, {1: [(2, {2}), (3, set()), (3, {2}), (4, set()), (4, {2}), (6, set())], 2: [(1, {1}), (4, set()), (6, {1}), (9, set())]})
        else:
            assert_equal({i: [(t, set(ts)) for t, ts in nsa] for i, nsa in lsa.neighbors_at()}, {1: [((2, True), {2}), ((5, True), set())], 2: [((1, True), {1}), ((3, True), set()), ((6, True), {1}), ((8, True), set())]})

        for w in [False, True]:
            if d:
                if w:
                    assert_equal({t: list(tc) for t, tc in lsa.degree_at(weights=w)}, {1: [(2, 1), (3, 2), (4, 1), (6, 0)], 2: [(1, 1), (4, 0), (6, 1), (9, 0)]})
                else:
                    assert_equal({t: list(tc) for t, tc in lsa.degree_at(weights=w)}, {1: [(2, 1), (6, 0)], 2: [(1, 1), (4, 0), (6, 1), (9, 0)]})
            else:
                assert_equal({t: list(tc) for t, tc in lsa.degree_at(weights=w)}, {1: [((2, True), 1), ((5, True), 0)], 2: [((1, True), 1), ((3, True), 0), ((6, True), 1), ((8, True), 0)]})

            na = lsa.degree_at(1, weights=w)
            assert isinstance(na, TimeCollection)
            if d:
                if w:
                    assert_equal([(t, ns) for t, ns in na], [(2, 1), (3, 2), (4, 1), (6, 0)])
                else:
                    assert_equal([(t, ns) for t, ns in na], [(2, 1), (6, 0)])
            else:
                assert_equal([(t, ns) for t, ns in na], [((2, True), 1), ((5, True), 0)])
            assert not na.instants
            assert len([a for a in na])

            assert_equal(lsa.degree_at(1, 2, weights=w), 1)
            assert_equal({i: n for i, n in lsa.degree_at(None, 2, 'out', weights=w)}, {1: 1, 2: 1})
            if d:
                if w:
                    assert_equal({i: [(t, ts) for t, ts in nsa] for i, nsa in lsa.degree_at(weights=w)}, {1: [(2, 1), (3, 2), (4, 1), (6, 0)], 2: [(1, 1), (4, 0), (6, 1), (9, 0)]})
                else:
                    assert_equal({i: [(t, ts) for t, ts in nsa] for i, nsa in lsa.degree_at(weights=w)}, {1: [(2, 1), (6, 0)], 2: [(1, 1), (4, 0), (6, 1), (9, 0)]})
            else:
                assert_equal({i: [(t, ts) for t, ts in nsa] for i, nsa in lsa.degree_at(weights=w)}, {1: [((2, True), 1), ((5, True), 0)], 2: [((1, True), 1), ((3, True), 0), ((6, True), 1), ((8, True), 0)]})

        nsa = NodeSetS({1})
        nsb = NodeSetS({2})
        ts = TimeSetDF([(1, 4)], discrete=d)
        nsma = TemporalNodeSetDF([(1, 1, 4)], discrete=d)
        nsmb = TemporalNodeSetDF([(1, 1, 4), (2, 1, 2)], discrete=d)
        if d:
            assert_equal(set(lsa.substream(nsa, nsb, ts)), {(1, 2, 2, 2, 1), (1, 2, 3, 3, 2), (1, 2, 4, 4, 1)})
            assert_equal(set(lsa.temporal_neighborhood(nsma, direction='out')), {(2, 2, 4)})
            assert_equal(set(lsa.induced_substream(nsmb)), {(1, 2, 2, 2, 1.0), (2, 1, 1, 2, 1.0)})
        else:
            assert_equal(set(lsa.substream(nsa, nsb, ts)), {(1, 2, 2, 4, 'both', 1)})
            assert_equal(set(lsa.temporal_neighborhood(nsma, direction='out')), {(2, 2, 4, 'both')})
            assert_equal(set(lsa.induced_substream(nsmb)), {(1, 2, 2, 2, 'both', 1.0), (2, 1, 1, 2, 'both', 1.0)})

        try:
            lsa | 1
        except UnrecognizedTemporalLinkSet:
            pass

        try:
            lsa & 1
        except UnrecognizedTemporalLinkSet:
            pass

        try:
            lsa - 1
        except UnrecognizedTemporalLinkSet:
            pass

        df = [(1, 2, 2, 10), (2, 3, 4, 16), (1, 3, 6, 12), (3, 4, 8, 16), (2, 4, 13, 17)]
        assert_equal(TemporalLinkSetDF(df).get_maximal_cliques(),
                     {(frozenset({2, 4}), (13, 17)),
                      (frozenset({3, 4}), (8, 16)),
                      (frozenset({2, 3, 4}), (13, 16)),
                      (frozenset({2, 3}), (4, 16)),
                      (frozenset({1, 2, 3}), (6, 10)),
                      (frozenset({1, 3}), (6, 12)),
                      (frozenset({1, 2}), (2, 10))})


def test_itemporal_link_set_df():
    assert_equal(ITemporalLinkSetDF().size, 0)
    assert_equal(ITemporalLinkSetDF([]).size, 0)
    assert ITemporalLinkSetDF([]).discrete

    for d in [False, True]:
        df = [(1, 2, 2), (1, 2, 3), (2, 1, 6), (2, 1, 3), (2, 1, 3)]
        lsa = ITemporalLinkSetDF(df, no_duplicates=False, discrete=d)

        assert bool(lsa)
        assert_equal(set(lsa), {(1, 2, 2), (1, 2, 3), (2, 1, 6), (2, 1, 3)})

        assert_equal(lsa.m, 2)
        assert_equal(lsa.size, 4*int(d))

        assert (1, 2, None) in lsa
        assert (1, None, 2) in lsa
        assert (5, None, None) not in lsa
        assert (1, 3, 1) not in lsa
        assert (1, 2, 2) in lsa

        assert_equal(lsa.duration_of((1, 2)), 2*int(d))
        assert_equal(lsa.duration_of((5, 1)), 0)
        assert_equal(dict(lsa.duration_of()), {(1, 2): 2*int(d), (2, 1): 2*int(d)})

        assert_equal(set(lsa.links_at(6)), {(2, 1)})
        assert_equal(set(lsa.links_at(3)), {(1, 2), (2, 1)})
        assert_equal(set(lsa.links_at(10)), set())

        la = lsa.links_at()
        assert isinstance(la, TimeGenerator)
        assert_equal([(t, list(ls)) for t, ls in la], [(2, [(1, 2)]), (3, [(1, 2), (2, 1)]), (6, [(2, 1)])])
        assert la.instants
        assert not len([a for a in la])

        assert_equal(set(lsa.times_of((1, 2))), {2, 3})
        assert_equal(set(lsa.times_of((10, 3))), set())
        assert_equal({l: set(s) for l, s in lsa.times_of()}, {(1, 2): {2, 3}, (2, 1): {3, 6}})

        assert_equal(lsa.m_at(3), 2)
        assert_equal(lsa.m_at(10), 0)
        assert_equal(list(lsa.m_at()), [(2, 1), (3, 2), (6, 1)])

        df = [(1, 2, 2), (1, 2, 4), (2, 1, 7), (2, 1, 3)]
        lsb = ITemporalLinkSetDF(df, discrete=d)
        assert_equal(set(lsb & lsa), set(lsa & lsb))
        assert isinstance(lsa & lsb, ITemporalLinkSetDF)
        assert_equal(set(lsa & lsb), {(1, 2, 2), (2, 1, 3)})

        assert isinstance(lsa | lsb, ITemporalLinkSetDF)
        assert_equal(set(lsb | lsa), set(lsa | lsb))
        assert_equal(set((lsb | lsa)), {(2, 1, 3), (2, 1, 7), (2, 1, 6), (1, 2, 2), (1, 2, 3), (1, 2, 4)})

        assert_equal(set(lsb - lsa), {(1, 2, 4), (2, 1, 7)})
        assert isinstance(lsb - lsa, ITemporalLinkSetDF)
        assert_equal(set(lsa - lsb), {(1, 2, 3), (2, 1, 6)})
        assert isinstance(lsa - lsb, ITemporalLinkSetDF)

        assert lsa.issuperset(lsa & lsb)
        assert lsb.issuperset(lsa & lsb)
        assert (lsa | lsb).issuperset(lsa)
        assert (lsa | lsb).issuperset(lsb)
        assert lsa.issuperset(lsa - lsb)

        assert_equal(set(lsa.neighbors_of(1)), {(2, 2), (2, 3)})
        assert isinstance(lsa.neighbors_of(1), ITemporalNodeSetDF)
        assert_equal(set(lsa.neighbors_of(1, 'in')), {(2, 6), (2, 3)})
        assert_equal(set(lsa.neighbors_of(1, 'both')), {(2, 2), (2, 3), (2, 6)})

        assert_equal({n: set(ns) for n, ns in lsa.neighbors_of()}, {1: {(2, 2), (2, 3)}, 2: {(1, 3), (1, 6)}})

        la = lsa.neighbors_at(1)
        assert isinstance(la, TimeGenerator)
        assert_equal([(t, set(ts)) for t, ts in la], [(2, {2}), (3, {2})])
        assert la.instants
        assert not len([a for a in la])

        assert_equal(set(lsa.neighbors_at(1, 2)), {2})
        assert_equal({i: set(nsa) for i, nsa in lsa.neighbors_at(None, 2, 'out')}, {1: {2}})
        assert_equal({i: [(t, set(ts)) for t, ts in nsa] for i, nsa in lsa.neighbors_at()}, {1: [(2, {2}), (3, {2})], 2: [(3, {1}), (6, {1})]})
        la = lsa.degree_at(1)
        assert isinstance(la, TimeGenerator)
        assert_equal(list(la), [(2, 1), (3, 1)])
        assert la.instants
        assert len([a for a in la])

        assert_equal(lsa.degree_at(1, 2), 1)
        assert_equal({i: n for i, n in lsa.degree_at(None, 2, 'out')}, {1: 1})
        assert_equal({i: [(t, ts) for t, ts in nsa] for i, nsa in lsa.degree_at()}, {1: [(2, 1), (3, 1)], 2: [(3, 1), (6, 1)]})

        nsa = NodeSetS({1})
        nsb = NodeSetS({2})
        ts = ITimeSetS([(2)], discrete=d)
        assert_equal(set(lsa.substream(nsa, nsb, ts)), {(1, 2, 2)})

        nsma = ITemporalNodeSetDF([(1, 2), (1, 3)], discrete=d)
        assert_equal(set(lsa.temporal_neighborhood(nsma, direction='out')), {(2, 2), (2, 3)})

        nsmb = ITemporalNodeSetDF([(1, 3), (2, 3)], discrete=d)
        assert_equal(set(lsa.induced_substream(nsmb)), {(1, 2, 3), (2, 1, 3)})

        try:
            lsa | 1
        except UnrecognizedTemporalLinkSet:
            pass

        try:
            lsa & 1
        except UnrecognizedTemporalLinkSet:
            pass

        try:
            lsa - 1
        except UnrecognizedTemporalLinkSet:
            pass

        df = [(1, 2, 2), (1, 2, 3), (2, 3, 2), (1, 3, 4), (3, 4, 3), (2, 4, 5)]
        assert_equal(list(ITemporalLinkSetDF(df, discrete=d).ego_betweeness(3, direction='both')), [(2, 0.0), (3, 1.0)])
        assert_equal(list(ITemporalLinkSetDF(df, discrete=d).closeness(2, direction='both')), [(2, 1.3333333333333333), (3, 1.5), (4, 1.0), (5, 0.0)])

        df = [(1, 2, 4), (1, 2, 8), (2, 3, 4), (1, 3, 6), (3, 4, 2), (2, 4, 3)]
        if d:
            assert_equal(ITemporalLinkSetDF(df, discrete=d).get_maximal_cliques(delta=3),
                         {(frozenset({1, 2}), (2, 5)),
                          (frozenset({2, 3}), (2, 5)),
                          (frozenset({3, 4}), (2, 3)),
                          (frozenset({1, 2, 3}), (4, 5)),
                          (frozenset({1, 3}), (4, 7)),
                          (frozenset({1, 2}), (6, 8)),
                          (frozenset({2, 3, 4}), (2, 3)),
                          (frozenset({2, 4}), (2, 4))})
        else:
            assert_equal(ITemporalLinkSetDF(df, discrete=d).get_maximal_cliques(delta=3),
                         set([(frozenset([1, 2]), (6.5, 8.0)), (frozenset([2, 3, 4]), (2.5, 3.5)),
                              (frozenset([1, 2]), (2.5, 5.5)), (frozenset([1, 2, 3]), (4.5, 5.5)),
                              (frozenset([2, 4]), (2.0, 4.5)), (frozenset([1, 3]), (4.5, 7.5)),
                              (frozenset([2, 3]), (2.5, 5.5)), (frozenset([3, 4]), (2.0, 3.5))]))

        df = [(1, 2, 2, 1), (1, 2, 3, 1), (2, 1, 6, 1), (2, 1, 3, 1)]
        lsa = ITemporalLinkSetDF(df, no_duplicates=False, weighted=True, discrete=d)
        assert bool(lsa)
        assert_equal(set(lsa), {(1, 2, 2, 1), (1, 2, 3, 1), (2, 1, 6, 1), (2, 1, 3, 1)})

        assert_equal(lsa.m, 2)
        assert_equal(lsa.size, 4*int(d))

        assert (1, 2, None) in lsa
        assert (1, None, 2) in lsa
        assert (5, None, None) not in lsa
        assert (1, 3, 1) not in lsa
        assert (1, 2, 2) in lsa

        assert_equal(lsa.duration_of((1, 2)), 2*int(d))
        assert_equal(lsa.duration_of((5, 1)), 0)
        assert_equal(dict(lsa.duration_of()), {(1, 2): 2*int(d), (2, 1): 2*int(d)})

        assert_equal(set(lsa.links_at(6)), {(2, 1, 1.0)})
        assert_equal(set(lsa.links_at(3)), {(1, 2, 1.0), (2, 1, 1.0)})
        assert_equal(set(lsa.links_at(10)), set())

        la = lsa.links_at()
        assert isinstance(la, TimeGenerator)
        assert_equal([(t, list(ls)) for t, ls in la], [(2, [(1, 2, 1.0)]), (3, [(1, 2, 1.0), (2, 1, 1.0)]), (6, [(2, 1, 1.0)])])
        assert la.instants
        assert not len([a for a in la])

        assert_equal(set(lsa.times_of((1, 2))), {2, 3})
        assert_equal(set(lsa.times_of((10, 3))), set())
        assert_equal({l: set(s) for l, s in lsa.times_of()}, {(1, 2): {2, 3}, (2, 1): {3, 6}})

        df = [(1, 2, 2, 1), (1, 2, 4, 1), (2, 1, 7, 2), (2, 1, 3, 2)]
        lsb = ITemporalLinkSetDF(df, weighted=True, discrete=d)
        assert_equal(set(lsb & lsa), set(lsa & lsb))
        assert isinstance(lsa & lsb, ITemporalLinkSetDF)
        assert_equal(set(lsa & lsb), {(2, 1, 3, 2), (1, 2, 2, 1)})

        assert isinstance(lsa | lsb, ITemporalLinkSetDF)
        assert_equal(set(lsb | lsa), set(lsa | lsb))
        assert_equal(set((lsb | lsa)), {(1, 2, 3, 1), (1, 2, 4, 1), (2, 1, 7, 2), (2, 1, 3, 3), (1, 2, 2, 2), (2, 1, 6, 1)})

        assert_equal(set(lsb - lsa), {(2, 1, 7, 2), (1, 2, 4, 1), (2, 1, 3, 1)})
        assert isinstance(lsb - lsa, ITemporalLinkSetDF)
        assert_equal(set(lsa - lsb), {(1, 2, 3, 1), (2, 1, 6, 1)})
        assert isinstance(lsa - lsb, ITemporalLinkSetDF)

        assert (lsa | lsb).issuperset(lsa)
        assert (lsa | lsb).issuperset(lsb)
        assert lsa.issuperset(lsa - lsb)

        assert_equal(set(lsa.neighbors_of(1)), {(2, 2), (2, 3)})
        assert isinstance(lsa.neighbors_of(1), ITemporalNodeSetDF)
        assert_equal(set(lsa.neighbors_of(1, 'in')), {(2, 6), (2, 3)})
        assert_equal(set(lsa.neighbors_of(1, 'both')), {(2, 2), (2, 3), (2, 6)})

        assert_equal({n: set(ns) for n, ns in lsa.neighbors_of()}, {1: {(2, 3), (2, 2)}, 2: {(1, 3), (1, 6)}})

        la = lsa.neighbors_at(1)
        assert isinstance(la, TimeGenerator)
        assert_equal({(t, tuple(s)) for t, s in la}, {(2, (2,)), (3, (2,))})
        assert la.instants
        assert not len([a for a in la])

        assert_equal(set(lsa.neighbors_at(1, 2)), {2})
        assert_equal({i: tuple(nsa) for i, nsa in lsa.neighbors_at(None, 2, 'out')}, {1: (2,)})
        assert_equal({i: set((t, tuple(s)) for t, s in nsa)for i, nsa in lsa.neighbors_at()}, {1: {(3, (2,)), (2, (2,))}, 2: {(3, (1,)), (6, (1,))}})

        for w in [False, True]:
            assert_equal(lsa.m_at(3, weights=w), 2)
            assert_equal(lsa.m_at(10, weights=w), 0)
            assert_equal(list(lsa.m_at(weights=w)), [(2, 1), (3, 2), (6, 1)])

            la = lsa.degree_at(1, weights=w)
            assert isinstance(la, TimeGenerator)
            assert_equal(list(la), [(2, 1), (3, 1)])
            assert la.instants
            assert len([a for a in la])

            assert_equal(lsa.degree_at(1, 2, weights=w), 1)
            assert_equal({i: n for i, n in lsa.degree_at(None, 2, 'out', weights=w)}, {1: 1})
            assert_equal({i: [(t, ts) for t, ts in nsa] for i, nsa in lsa.degree_at(weights=w)}, {1: [(2, 1), (3, 1)], 2: [(3, 1), (6, 1)]})

        nsa = NodeSetS({1})
        nsb = NodeSetS({2})
        ts = ITimeSetS([(2)])
        assert_equal(set(lsa.substream(nsa, nsb, ts)), {(1, 2, 2, 1)})

        nsma = ITemporalNodeSetDF([(1, 2), (1, 3)], discrete=d)
        assert_equal(set(lsa.temporal_neighborhood(nsma, direction='out')), {(2, 2), (2, 3)})

        nsmb = ITemporalNodeSetDF([(1, 3), (2, 3)], discrete=d)
        assert_equal(set(lsa.induced_substream(nsmb)), {(1, 2, 3, 1), (2, 1, 3, 1)})

        try:
            lsa | 1
        except UnrecognizedTemporalLinkSet:
            pass

        try:
            lsa & 1
        except UnrecognizedTemporalLinkSet:
            pass

        try:
            lsa - 1
        except UnrecognizedTemporalLinkSet:
            pass

        df = [(1, 2, 4, 1), (1, 2, 8, 2), (2, 3, 4, 1), (1, 3, 6, 1), (3, 4, 2, 1), (2, 4, 3, 2)]
        if d:
            assert_equal(ITemporalLinkSetDF(df, weighted=True, discrete=d).get_maximal_cliques(delta=3),
                         {(frozenset({1, 2}), (2, 5)),
                          (frozenset({2, 3}), (2, 5)),
                          (frozenset({3, 4}), (2, 3)),
                          (frozenset({1, 2, 3}), (4, 5)),
                          (frozenset({1, 3}), (4, 7)),
                          (frozenset({1, 2}), (6, 8)),
                          (frozenset({2, 3, 4}), (2, 3)),
                          (frozenset({2, 4}), (2, 4))})
        else:
            assert_equal(ITemporalLinkSetDF(df, weighted=True, discrete=d).get_maximal_cliques(delta=3),
                         set([(frozenset([1, 2]), (6.5, 8.0)), (frozenset([2, 3, 4]), (2.5, 3.5)),
                              (frozenset([1, 2]), (2.5, 5.5)), (frozenset([1, 2, 3]), (4.5, 5.5)),
                              (frozenset([2, 4]), (2.0, 4.5)), (frozenset([1, 3]), (4.5, 7.5)),
                              (frozenset([2, 3]), (2.5, 5.5)), (frozenset([3, 4]), (2.0, 3.5))]))


        df = [(1, 2, 2, 1), (1, 2, 3, 1), (2, 3, 2, 1), (1, 3, 4, 1), (3, 4, 3, 1), (2, 4, 5, 1)]
        assert_equal(list(ITemporalLinkSetDF(df, weighted=True, discrete=d).ego_betweeness(3, direction='both')), [(2, 0.0), (3, 1.0)])
        assert_equal(list(ITemporalLinkSetDF(df, weighted=True, discrete=d).closeness(2, direction='both')), [(2, 1.3333333333333333), (3, 1.5), (4, 1.0), (5, 0.0)])

if __name__ == "__main__":
    test_temporal_link_set_df()
    test_itemporal_link_set_df()
