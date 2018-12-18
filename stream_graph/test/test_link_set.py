"""Test file for link set."""
from pandas import DataFrame
from stream_graph import LinkSetDF
from stream_graph.exceptions import UnrecognizedLinkSet
from stream_graph.exceptions import UnrecognizedDirection

def test_link_set_df():
    links_a = [(1, 2), (2, 3), (3, 1), (1, 2)]
    lsa = LinkSetDF(links_a)

    assert bool(lsa)
    assert not bool(LinkSetDF({}))
    assert not bool(LinkSetDF())

    assert lsa.size == 3
    assert LinkSetDF().size == 0

    assert (1, 2) in lsa
    assert (2, 3) in lsa
    assert (3, 1) in lsa
    assert (1, 4) not in lsa

    assert isinstance(lsa.df, DataFrame)
    assert list(lsa.df.columns) == ['u', 'v']
    assert list(lsa) == sorted(list(set(links_a)))

    links_b = {(2, 3), (3, 1), (1, 4), (2, 3)}
    lsb = LinkSetDF(links_b)
    assert list(lsb & lsa) == list(lsa & lsb)
    assert (lsb & lsa).size == 2
    assert list((lsb & lsa)) == sorted([(2, 3), (3, 1)])

    assert list(lsb | lsa) == list(lsa | lsb)
    assert (lsb | lsa).size == 4
    assert list((lsb | lsa)) == sorted([(1, 2), (1, 4), (2, 3), (3, 1)])

    assert list(lsb - lsa) == sorted([(1, 4)])
    assert (lsb - lsa).size == 1
    assert list(lsa - lsb) == sorted([(1, 2)])
    assert (lsb - lsa).size == 1

    assert lsa.degree(1, 'out') == 1
    assert lsa.degree(1, 'in') == 1
    assert lsa.degree(1, 'both') == 2
    assert set(lsa.neighbors(1, 'out')) == {2}
    assert set(lsa.neighbors(1, 'in')) == {3}
    assert set(lsa.neighbors(1, 'both')) == {2, 3}

    try:
        lsa.degree(1, 'tdod')
    except UnrecognizedDirection:
        pass

    try:
        lsa.neighbors(1, 'tdod')
    except UnrecognizedDirection:
        pass

    assert lsa.issuperset(lsa & lsb)
    assert lsb.issuperset(lsa & lsb)
    assert (lsa | lsb).issuperset(lsa)
    assert (lsa | lsb).issuperset(lsb)
    assert lsa.issuperset(lsa - lsb)

    try:
        lsa | 1
    except UnrecognizedLinkSet:
        pass

    try:
        lsa & 1
    except UnrecognizedLinkSet:
        pass

    try:
        lsa - 1
    except UnrecognizedLinkSet:
        pass


if __name__ == "__main__":
    test_link_set_df()
