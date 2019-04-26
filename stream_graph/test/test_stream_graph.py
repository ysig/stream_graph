"""Test file for stream graph."""
from pandas import DataFrame
from stream_graph import TemporalLinkSetDF
from stream_graph import StreamGraph
from stream_graph.exceptions import UnrecognizedLinkSet
from stream_graph.exceptions import UnrecognizedDirection

def test_stream_graph():
    for w in [False, True]:
        df = [(1, 2, 2, 3), (1, 2, 3, 5), (2, 1, 6, 8), (2, 1, 1, 3)]
        sga = TemporalLinkSetDF(df, disjoint_intervals=False, weighted=w).as_stream_graph_basic

        assert isinstance(sga, StreamGraph)
        assert bool(sga)
        assert not bool(StreamGraph())

        assert sga.n == 2.0
        assert sga.m == 1.0

        assert sga.temporal_nodeset_coverage == 1.0
        assert sga.temporal_linkset_coverage == 1.0

        assert sga.time_coverage_node(1) == 1.0
        assert dict(sga.time_coverage_node()) == {1: 1.0, 2: 1.0}
        
        assert dict(sga.time_coverage_link()) == {(1, 2): 0.42857142857142855, (2, 1): 0.5714285714285714}
        assert sga.time_coverage_link((1, 2)) == 0.42857142857142855
        assert sga.time_coverage_link((1, 2), 'in') == 0.5714285714285714
        assert sga.time_coverage_link((1, 2), 'both') == 0.8571428571428571

        assert sga.node_coverage_at(2.5) == 1.0
        assert sga.node_coverage_at(10) == 0.
        assert sga.node_coverage_at(2.5) == 1.0
        assert list(sga.node_coverage_at()) == [(1, 1.0), (8, 0.0)]

        assert sga.link_coverage_at(2.5) == 1.0
        assert list(sga.link_coverage_at()) == [(1, 0.5), (2, 1.0), (3, 0.5), (5, 0.0), (6, 0.5), (8, 0.0)]

        assert sga.neighbor_coverage(1) == 0.42857142857142855
        assert sga.neighbor_coverage(1, 'in') == 0.5714285714285714
        assert sga.neighbor_coverage(1, 'both') == 0.8571428571428571
        assert dict(sga.neighbor_coverage()) == {1: 0.42857142857142855, 2: 0.5714285714285714}

        assert sga.neighbor_coverage_at(1, 2.5, weights=w) == 0.5
        assert sga.neighbor_coverage_at(1, 2.5, 'in', weights=w) == 0.5
        assert sga.neighbor_coverage_at(1, 2.5, 'both', weights=w) == 0.5 + int(w)*0.5 # should this be different?
        assert dict(sga.neighbor_coverage()) == {1: 0.42857142857142855, 2: 0.5714285714285714}

        assert sga.mean_degree_at(2, weights=w) == 1.0
        assert list(sga.mean_degree_at(weights=w)) == [(1, 0.5), (2, 1.0), (3, 0.5), (5, 0.0), (6, 0.5), (8, 0.0)]

if __name__ == "__main__":
    test_stream_graph()
