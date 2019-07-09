"""Test file for stream graph."""
from pandas import DataFrame
from stream_graph import TemporalLinkSetDF
from stream_graph import StreamGraph
from stream_graph.exceptions import UnrecognizedLinkSet
from stream_graph.exceptions import UnrecognizedDirection
from nose.tools import assert_equal


def test_stream_graph():
    for d in [False, True]:
        for w in [False, True]:
            if w:
                df = [(1, 2, 2, 3, 1), (1, 2, 3, 5, 1), (2, 1, 6, 8, 1), (2, 1, 1, 3, 1)]
            else:
                df = [(1, 2, 2, 3), (1, 2, 3, 5), (2, 1, 6, 8), (2, 1, 1, 3)]

            sga = TemporalLinkSetDF(df, disjoint_intervals=False, weighted=w, discrete=d).as_stream_graph_basic

            assert isinstance(sga, StreamGraph)
            assert bool(sga)
            assert not bool(StreamGraph())

            assert_equal(sga.n, 2.0)
            assert_equal(sga.m, 1.0 + int(d)*0.25)

            assert_equal(sga.temporal_nodeset_coverage, 1.0)
            assert_equal(sga.temporal_linkset_coverage, 1.0 + int(d)*0.25)

            assert_equal(sga.time_coverage_node(1), 1.0)
            assert_equal(dict(sga.time_coverage_node()), {1: 1.0, 2: 1.0})

            if d:
                assert_equal(dict(sga.time_coverage_link()), {(1, 2): 0.5, (2, 1): 0.75})
                assert_equal(sga.time_coverage_link((1, 2)), 0.5)
                assert_equal(sga.time_coverage_link((1, 2), direction='in'), 0.75)
                assert_equal(sga.time_coverage_link((1, 2), direction='both'), 1.0)
            else:
                assert_equal(dict(sga.time_coverage_link()), {(1, 2): 0.42857142857142855, (2, 1): 0.5714285714285714})
                assert_equal(sga.time_coverage_link((1, 2)), 0.42857142857142855)
                assert_equal(sga.time_coverage_link((1, 2), direction='in'), 0.5714285714285714)
                assert_equal(sga.time_coverage_link((1, 2), direction='both'), 0.8571428571428571)


            t = (2 if d else 2.5)
            assert_equal(sga.node_coverage_at(t), 1.0)
            assert_equal(sga.node_coverage_at(10), 0.)
            assert_equal(sga.node_coverage_at(t), 1.0)

            assert_equal(sga.link_coverage_at(t), 1.0)
            if d:
                assert_equal(list(sga.node_coverage_at()), [(1, 1.0), (9, 0.0)])
                assert_equal(list(sga.link_coverage_at()),  [(1, 0.5), (2, 1.0), (4, 0.5), (9, 0.0)])
            else:
                assert_equal(list(sga.node_coverage_at()), [((1, True), 1.0), ((8, not w), 0.0)])
                assert_equal(list(sga.link_coverage_at()), [((1, True), 0.5), ((2, True), 1.0), ((3, w), 0.5), ((5, w), 0.0), ((6, True), 0.5), ((8, False), 0.0)])

            if d:
                assert_equal(sga.neighbor_coverage(1), 0.5)
                assert_equal(sga.neighbor_coverage(1, direction='in'), 0.75)
                assert_equal(sga.neighbor_coverage(1, direction='both'), 1.0)
                assert_equal(dict(sga.neighbor_coverage()), {1: 0.5, 2: 0.75})
            else:
                assert_equal(sga.neighbor_coverage(1), 0.42857142857142855)
                assert_equal(sga.neighbor_coverage(1, direction='in'), 0.5714285714285714)
                assert_equal(sga.neighbor_coverage(1, direction='both'), 0.8571428571428571)
                assert_equal(dict(sga.neighbor_coverage()), {1: 0.42857142857142855, 2: 0.5714285714285714})

            assert_equal(sga.neighbor_coverage_at(1, t, weights=w), 0.5)
            assert_equal(sga.neighbor_coverage_at(1, t, direction='in', weights=w), 0.5)
            assert_equal(sga.neighbor_coverage_at(1, t, direction='both', weights=w), 0.5 + int(w)*0.5) # should this be different?
            if d:
                assert_equal(dict(sga.neighbor_coverage()), {1: 0.5, 2: 0.75})
            else:
                assert_equal(dict(sga.neighbor_coverage()), {1: 0.42857142857142855, 2: 0.5714285714285714})

            assert_equal(sga.mean_degree_at(2, weights=w), 1.0)
            if d:
                if w:
                    assert_equal(list(sga.mean_degree_at(weights=w)), [(1, 0.5), (2, 1.0), (3, 1.5), (4, 0.5), (9, 0.0)])
                else:
                    assert_equal(list(sga.mean_degree_at(weights=w)), [(1, 0.5), (2, 1.0), (4, 0.5), (9, 0.0)])
            else:
                assert_equal(list(sga.mean_degree_at(weights=w)), [((1, True), 0.5), ((2, True), 1.0), ((3, w), 0.5), ((5, w), 0.0), ((6, True), 0.5), ((8, False), 0.0)])

if __name__ == "__main__":
    test_stream_graph()
