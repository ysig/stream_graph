"""Test file for link set."""
from stream_graph import Graph
# from stream_graph.exceptions import UnrecognizedLinkSet
# from stream_graph.exceptions import UnrecognizedDirection
from nose.tools import assert_equal


def test_graph():
    nodes_a = [1, 2, 3, 4]
    links_a = [(1, 2), (2, 3), (3, 1)]
    for w in [False, True]:
        g = Graph(nodes_a, links_a, weighted=w)
        assert bool(g)
        assert not bool(Graph())
        assert_equal(g.n, 4)
        assert_equal(g.m, 3)
        assert_equal(g.wm, 3)

        assert_equal(g.total_coverage, 3 / 16.0)
        assert_equal(g.weighted_total_coverage, 3 / 16.0)
        for ww in [False, True]:
            assert_equal(g.neighbor_coverage_of(2, weights=ww), 1 / 16.0)
            assert_equal(g.neighbor_coverage_of(2, 'in', weights=ww), 1 / 16.0)
            assert_equal(g.neighbor_coverage_of(2, 'both', weights=ww), 1 / 8.0)
            assert_equal(set(g.neighbor_coverage_of(weights=ww)), {(1, 0.0625), (2, 0.0625), (3, 0.0625)})
            assert_equal(set(g.neighbor_coverage_of(direction='both', weights=ww)), {(1, 0.125), (2, 0.125), (3, 0.125)})


if __name__ == "__main__":
    test_graph()
