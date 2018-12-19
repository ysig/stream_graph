"""Test file for link set."""
from stream_graph import Graph
from stream_graph.exceptions import UnrecognizedLinkSet
from stream_graph.exceptions import UnrecognizedDirection

def test_graph():
    nodes_a = [1, 2, 3, 4]
    links_a = [(1, 2), (2, 3), (3, 1)]
    g = Graph(nodes_a, links_a)
    
    assert bool(g)
    assert not bool(Graph())
    assert g.n == 4
    assert g.m == 3

    assert g.coverage() == 3/16.0
    assert g.coverage(2) == 1/16.0
    assert g.coverage(2, 'in') == 1/16.0
    assert g.coverage(2, 'both') == 1/8.0

if __name__ == "__main__":
    test_graph()
