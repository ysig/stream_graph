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
    print(sga.m)

    print(sga.nodestream_coverage)
    print(sga.linkstream_coverage)

    print(sga.time_coverage(1))
    print(sga.time_coverage(1, 2))
    print(sga.time_coverage(1, 2, 'in'))
    print(sga.time_coverage(1, 2, 'both'))
    print(sga.node_coverage(2.5))
    print(sga.node_coverage(10))
    print(sga.node_coverage(2.5))
    print(sga.node_coverage(10))
    print(sga.neighbor_coverage(1))
    print(sga.neighbor_coverage(1, 'in'))
    print(sga.neighbor_coverage(1, 'both'))
    print(sga.neighbor_coverage_at(1, 2.5))
    print(sga.neighbor_coverage_at(1, 2.5, 'in'))
    print(sga.neighbor_coverage_at(1, 2.5, 'both'))
    print(sga.total_density)
    print(sga.density(1))
    print(sga.density(1, 2))
    print(sga.density(1, 2, 'in'))
    print(sga.density(1, 2, 'both'))
    print(sga.density(2.5))

    print(contribution)

if __name__ == "__main__":
    test_stream_graph()
