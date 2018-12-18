"""Test file for node set."""
from stream_graph import NodeSetS
from stream_graph.exceptions import UnrecognizedNodeSet


def test_node_set_s():
    nodes_a = {1, 2, 4}
    nsa = NodeSetS(nodes_a)

    assert bool(nsa)
    assert not bool(NodeSetS({}))
    assert not bool(NodeSetS())

    assert nsa.size == 3
    assert NodeSetS().size == 0

    assert 1 in nsa
    assert 2 in nsa
    assert 4 in nsa
    assert 5 not in nsa

    assert nsa.nodes == nodes_a

    nodes_b = range(1, 4)
    nsb = NodeSetS(nodes_b)
    assert (nsb & nsa).nodes == (nsa & nsb).nodes
    assert (nsb & nsa).size == 2
    assert (nsb & nsa).nodes == {1, 2}

    assert (nsb | nsa).nodes == (nsa | nsb).nodes
    assert (nsb | nsa).size == 4
    assert (nsb | nsa).nodes == {1, 2, 3, 4}

    assert (nsb - nsa).nodes == {3}
    assert (nsb - nsa).size == 1
    assert (nsa - nsb).nodes == {4}
    assert (nsb - nsa).size == 1

    assert all(isinstance(i, int) for i in nsa)
    assert set(iter(nsa)) == nsa.nodes
    assert set(iter(nsa)) == nodes_a

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
