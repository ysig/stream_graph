"""Test file for node set."""
from stream_graph import NodeSetS
from stream_graph.exceptions import UnrecognizedNodeSet
from nose.tools import assert_equal


def test_node_set_s():
    nodes_a = {1, 2, 4}
    nsa = NodeSetS(nodes_a)

    assert bool(nsa)
    assert not bool(NodeSetS({}))
    assert not bool(NodeSetS())

    assert_equal(nsa.size, 3)
    assert_equal(NodeSetS().size, 0)

    assert 1 in nsa
    assert 2 in nsa
    assert 4 in nsa
    assert 5 not in nsa

    assert_equal(nsa.nodes, nodes_a)

    nodes_b = range(1, 4)
    nsb = NodeSetS(nodes_b)
    assert_equal((nsb & nsa).nodes, (nsa & nsb).nodes)
    assert_equal((nsb & nsa).size, 2)
    assert_equal((nsb & nsa).nodes, {1, 2})

    assert_equal((nsb | nsa).nodes, (nsa | nsb).nodes)
    assert_equal((nsb | nsa).size, 4)
    assert_equal((nsb | nsa).nodes, {1, 2, 3, 4})

    assert_equal((nsb - nsa).nodes, {3})
    assert_equal((nsb - nsa).size, 1)
    assert_equal((nsa - nsb).nodes, {4})
    assert_equal((nsb - nsa).size, 1)

    assert all(isinstance(i, int) for i in nsa)
    assert_equal(set(iter(nsa)), nsa.nodes)
    assert_equal(set(iter(nsa)), nodes_a)

    try:
        nsa | 1
    except UnrecognizedNodeSet:
        pass

    try:
        nsa & 1
    except UnrecognizedNodeSet:
        pass

    try:
        nsa - 1
    except UnrecognizedNodeSet:
        pass


if __name__ == "__main__":
    test_node_set_s()
