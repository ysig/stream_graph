"""Test file for time set."""
from itertools import product
from pandas import DataFrame
from stream_graph import NodeSetS
from stream_graph import TimeSetDF
from stream_graph import NodeStreamB
from stream_graph import NodeStreamDF
from stream_graph.exceptions import UnrecognizedTimeSet
from stream_graph.exceptions import UnrecognizedNodeStream


def test_node_stream_b():
    nodeset = NodeSetS({1, 2, 3, 5})
    timeset = TimeSetDF([(1, 2), (3, 5), (6, 7)])
    nsa = NodeStreamB(nodeset, timeset)

    assert bool(nsa)
    assert not bool(NodeStreamB([], []))
    assert not bool(NodeStreamB([], [(1, 2)]))
    assert not bool(NodeStreamB([1], []))
    assert not bool(NodeStreamB())

    assert set(nsa.nodeset) == set(nodeset)
    assert set(nsa.timeset) == set(timeset)

    assert set(nsa) == {(n, ts, tf) for n in nodeset for ts, tf in timeset}

    assert nsa.n == 4
    assert nsa.size == 16
    assert nsa.total_time == 4
    assert NodeStreamB().size == 0
    assert NodeStreamB([], []).size == 0
    assert NodeStreamB([], [(1, 2)]).size == 0
    assert NodeStreamB([1], []).size == 0

    assert (2, None) in nsa
    assert (None, 3.4) in nsa
    assert (None, None) not in nsa
    assert (2, 3.4) in nsa
    assert (2, (3.4, 4)) in nsa

    assert nsa.node_duration(4) == 0
    assert nsa.node_duration(1) == 4

    assert nsa.common_time(1) == 12
    assert nsa.common_time(2, 3) == 4
    assert nsa.common_time(7) == .0
    assert nsa.common_time(2, 8) == .0

    assert set(nsa.nodes_at(1)) == {1, 2, 3, 5}
    assert set(nsa.nodes_at((1, 1.5))) == {1, 2, 3, 5}
    assert set(nsa.nodes_at(2.5)) == set()

    assert set(nsa.times_of(1)) == {(1, 2), (3, 5), (6, 7)}
    assert set(nsa.times_of(10)) == set()

    assert nsa.n_at(6) == 4
    assert nsa.n_at(10) == 0


    nodeset = NodeSetS({2, 3, 4})
    timeset = TimeSetDF([(1, 3), (4, 8)])
    nsb = NodeStreamB(nodeset, timeset)
    assert set(nsb & nsa) == set(nsa & nsb)
    assert isinstance(nsa & nsb, NodeStreamB)
    assert set(nsa & nsb) == {(n, ts, tf) for n in {2, 3} for ts, tf in [(1, 2), (3, 3), (4, 5), (6, 7)]}    
    assert (nsb & nsa).size == 6

    assert set(nsb | nsa) == set(nsa | nsb)
    assert isinstance(nsa | nsb, NodeStreamB)
    assert set((nsb | nsa)) == {(n, 1, 8) for n in {1, 2, 3, 4, 5}}
    assert (nsb | nsa).size == 35

    diff_ba = set((n, ts, tf) for e in [([4], [(1, 3), (4, 8)]), ([2, 3], [(2, 3), (5, 6), (7, 8)])] for n in e[0] for ts, tf in e[1])
    assert set(nsb - nsa) == diff_ba
    assert (nsb - nsa).size == 12
    diff_ab = set((n, ts, tf) for e in [([1, 5], [(1, 2), (3, 5), (6, 7)]), ([2, 3], [(3, 4)])] for n in e[0] for ts, tf in e[1])
    assert set(nsa - nsb) == diff_ab
    assert (nsa - nsb).size == 10

    assert nsa.issuperset(nsa & nsb)
    assert nsb.issuperset(nsa & nsb)
    assert (nsa | nsb).issuperset(nsa)
    assert (nsa | nsb).issuperset(nsb)
    assert nsa.issuperset(nsa - nsb)

    try:
        nsa | 1
    except UnrecognizedNodeStream:
        pass

    try:
        nsa & 1
    except UnrecognizedNodeStream:
        pass

    try:
        nsa - 1
    except UnrecognizedNodeStream:
        pass


def test_node_stream_df():
    df = [(1, 2, 3), (1, 3, 5), (1, 6, 8), (2, 1, 3)]
    nsa = NodeStreamDF(df, disjoint_intervals=False)

    assert bool(nsa)
    assert not bool(NodeStreamDF([]))
    assert not bool(NodeStreamDF())

    assert set(nsa) == {(1, 2, 5), (1, 6, 8), (2, 1, 3)}

    assert nsa.n == 2
    assert nsa.size == 7
    assert nsa.total_time == 6
    assert NodeStreamDF().size == 0
    assert NodeStreamDF([]).size == 0

    assert (2, None) in nsa
    assert (None, 3.4) in nsa
    assert (None, None) not in nsa
    assert (2, 3.4) not in nsa
    assert (2, (3.4, 4)) not in nsa

    assert nsa.node_duration(1) == 5
    assert nsa.node_duration(4) == 0

    assert nsa.common_time(1) == 1
    assert nsa.common_time(1, 2) == 1
    assert nsa.common_time(1, 2) == nsa.common_time(2, 1)
    assert nsa.common_time(7) == .0
    assert nsa.common_time(2, 8) == .0

    assert set(nsa.nodes_at(1)) == {2}
    assert set(nsa.nodes_at((2, 2.5))) == {1, 2}
    assert set(nsa.nodes_at(10)) == set()

    assert set(nsa.times_of(1)) == {(2, 5), (6, 8)}
    assert set(nsa.times_of(10)) == set()

    assert nsa.n_at(2) == 2
    assert nsa.n_at(10) == 0

    df = [(1, 1, 4), (1, 6, 7), (2, 2.5, 2.6)]
    nsb = NodeStreamDF(df)
    assert set(nsb & nsa) == set(nsa & nsb)
    assert isinstance(nsa & nsb, NodeStreamDF)
    assert set(nsa & nsb) == {(1, 2, 4), (1, 6, 7), (2, 2.5, 2.6)}
    assert (nsa & nsb).size == 3.1

    assert set(nsb | nsa) == set(nsa | nsb)
    assert isinstance(nsa | nsb, NodeStreamDF)
    assert set((nsb | nsa)) == {(1, 1, 5), (1, 6, 8), (2, 1, 3)}
    assert (nsb | nsa).size == 8

    assert set(nsb - nsa) == {(1, 1, 2)}
    assert (nsb - nsa).size == 1
    assert isinstance(nsb - nsa, NodeStreamDF)
    assert set(nsa - nsb) == {(1, 4, 5), (1, 7, 8), (2, 1, 2.5), (2, 2.6, 3)}
    assert isinstance(nsa - nsb, NodeStreamDF)
    assert (nsa - nsb).size == 3.9

    assert nsa.issuperset(nsa & nsb)
    assert nsb.issuperset(nsa & nsb)
    assert (nsa | nsb).issuperset(nsa)
    assert (nsa | nsb).issuperset(nsb)
    assert nsa.issuperset(nsa - nsb)

    try:
        nsa | 1
    except UnrecognizedNodeStream:
        pass

    try:
        nsa & 1
    except UnrecognizedNodeStream:
        pass

    try:
        nsa - 1
    except UnrecognizedNodeStream:
        pass

def test_node_stream_op_b_df():
    nsa = NodeStreamB({1, 2}, [(1, 2), (4, 7)])
    nsb = NodeStreamDF([(1, 2, 5), (1, 6, 8), (2, 1, 3)])
    assert set(nsb & nsa) == set(nsa & nsb)
    assert type(nsb & nsa) is type(nsa & nsb)
    assert isinstance(nsa & nsb, NodeStreamDF)
    assert set(nsa & nsb) == {(1, 2, 2), (1, 6, 7), (2, 1, 2), (1, 4, 5)}
    assert (nsb & nsa).size == 3

    assert set(nsb | nsa) == set(nsa | nsb)
    assert type(nsb | nsa) is type(nsa | nsb)
    assert isinstance(nsa | nsb, NodeStreamDF)
    assert set((nsb | nsa)) == {(1, 1, 8), (2, 1, 3), (2, 4, 7)}
    assert (nsb | nsa).size == 12

    assert set(nsb - nsa) == { (1, 2, 4), (1, 7, 8), (2, 2, 3)}
    assert (nsb - nsa).size == 4
    assert isinstance(nsb - nsa, NodeStreamDF) and type(nsb - nsa) is type(nsa - nsb)
    assert set(nsa - nsb) == {(1, 1, 2), (1, 5, 6), (2, 4, 7)}
    assert (nsa - nsb).size == 5

    assert nsa.issuperset(nsa & nsb)
    assert nsb.issuperset(nsa & nsb)
    assert (nsa | nsb).issuperset(nsa)
    assert (nsa | nsb).issuperset(nsb)
    assert nsa.issuperset(nsa - nsb)


if __name__ == "__main__":
    test_node_stream_b()
    test_node_stream_df()
    test_node_stream_op_b_df()
