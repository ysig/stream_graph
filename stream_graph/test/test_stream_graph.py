"""Test file for link set."""
from pandas import DataFrame
from stream_graph import LinkStreamDF
from stream_graph import StreamGraph
from stream_graph.exceptions import UnrecognizedLinkSet
from stream_graph.exceptions import UnrecognizedDirection

def test_stream_graph():
    df = [(1, 2, 2, 3), (1, 2, 3, 5), (2, 1, 6, 8), (2, 1, 1, 3)]
    sga = LinkStreamDF(df, disjoint_intervals=False).as_stream_graph_minimal

    assert isinstance(sga, StreamGraph)
    assert bool(sga)
    assert not bool(StreamGraph())

    assert sga.n == 2.0
    assert sga.m == 1.0

    assert sga.nodestream_coverage == 0.25
    assert sga.linkstream_coverage == 0.25

    assert sga.time_coverage(1) == 1.0
    assert sga.time_coverage(1, 2) == 0.42857142857142855
    assert sga.time_coverage(1, 2, 'in') == 0.5714285714285714
    assert sga.time_coverage(1, 2, 'both') == 1.0

    assert sga.node_coverage(2.5) == 1.0
    assert sga.node_coverage(10) == 0.
    assert sga.node_coverage(2.5) == 1.0

    assert sga.neighbor_coverage(1) == 0.21428571428571427
    assert sga.neighbor_coverage(1, 'in') == 0.2857142857142857
    assert sga.neighbor_coverage(1, 'both') == 0.42857142857142855

    assert sga.neighbor_coverage_at(1, 2.5) == 0.5
    assert sga.neighbor_coverage_at(1, 2.5, 'in') == 0.5
    assert sga.neighbor_coverage_at(1, 2.5, 'both') == 0.5

    assert sga.total_density == 0.5
    assert sga.density(1) == 1.5
    assert sga.density(1, 2) == 1.5
    assert sga.density(1, 2, 'in') == 2.0
    assert sga.density(1, 2, 'both') == 3.5
    assert sga.density(2.5) == 0.

    assert sga.contribution(1) == 0.2857142857142857
    assert sga.contribution(1, 2) == 0.42857142857142855
    assert sga.contribution(1, 2, 'in') == 0.5714285714285714
    assert sga.contribution(1, 2, 'both') == 1.0

if __name__ == "__main__":
    test_stream_graph()
