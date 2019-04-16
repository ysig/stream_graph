"""Test file for link set."""
from pandas import DataFrame
from stream_graph import LinkSetDF
from stream_graph.exceptions import UnrecognizedLinkSet
from stream_graph.exceptions import UnrecognizedDirection

def test_link_set_df():
    links_a = [(1, 2), (2, 3), (3, 1), (1, 2)]
    lsa = LinkSetDF(links_a, no_duplicates=False)

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
    lsb = LinkSetDF(links_b, no_duplicates=False)
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

    assert dict(lsa.degree(direction='out')) == {1: 1, 2: 1, 3: 1}
    assert dict(lsa.degree(direction='in')) == {2: 1, 3: 1, 1: 1}
    assert dict(lsa.degree(direction='both')) == {1: 2, 2: 2, 3: 2}
    
    assert set(lsa.neighbors(1, 'out')) == {2}
    assert set(lsa.neighbors(1, 'in')) == {3}
    assert set(lsa.neighbors(1, 'both')) == {2, 3}
    assert list((a, set(b)) for (a, b) in lsa.neighbors(direction='out')) == [(1, {2}), (2, {3}), (3, {1})]
    assert list((a, set(b)) for (a, b) in lsa.neighbors(direction='in')) == [(2, {1}), (3, {2}), (1, {3})]
    assert list((a, set(b)) for (a, b) in lsa.neighbors(direction='both')) == [(1, {2, 3}), (2, {1, 3}), (3, {1, 2})]

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

def test_link_set_weighted():
    links_a = [(1, 2, 1), (2, 3, 1), (3, 1, 1), (1, 2, 1)]
    lsa = LinkSetDF(links_a, no_duplicates=False, weighted=True)

    assert bool(lsa)
    assert not bool(LinkSetDF({}, weighted=True))
    assert not bool(LinkSetDF(weighted=True))

    assert lsa.size == 3
    assert LinkSetDF(weighted=True).size == 0

    assert (1, 2) in lsa
    assert (2, 3) in lsa
    assert (3, 1) in lsa
    assert (1, 4) not in lsa

    assert isinstance(lsa.df, DataFrame)
    assert list(lsa.df.columns) == ['u', 'v', 'w']
    assert list(lsa) == [(1, 2, 2), (2, 3, 1), (3, 1, 1)]

    links_b = [(2, 3, 1), (3, 1, 2), (1, 4, 2), (2, 3, 1)]
    lsb = LinkSetDF(links_b, no_duplicates=False, weighted=True)

    assert list(lsb & lsa) == list(lsa & lsb)
    assert (lsb & lsa).size == 2
    assert list((lsb & lsa)) == sorted([(2, 3, 3), (3, 1, 3)])

    assert list(lsb | lsa) == list(lsa | lsb)
    assert (lsb | lsa).size == 4
    assert list((lsb | lsa)) == sorted([(1, 2, 2), (1, 4, 2), (2, 3, 3), (3, 1, 3)])

    assert list(lsb - lsa) == sorted([(1, 4, 2)])
    assert (lsb - lsa).size == 1
    assert list(lsa - lsb) == sorted([(1, 2, 2)])
    assert (lsb - lsa).size == 1

    assert lsa.degree(1, 'out', weighted=True) == 2
    assert lsa.degree(1, 'in', weighted=True) == 1
    assert lsa.degree(1, 'both', weighted=True) == 3

    assert dict(lsa.degree(direction='out', weighted=True)) == {1: 2, 2: 1, 3: 1}
    assert dict(lsa.degree(direction='in', weighted=True)) == {2: 2, 3: 1, 1: 1}
    assert dict(lsa.degree(direction='both', weighted=True)) == {1: 3, 2: 3, 3: 2}

    assert lsa.degree(1, 'out') == 1
    assert lsa.degree(1, 'in') == 1
    assert lsa.degree(1, 'both') == 2

    assert dict(lsa.degree(direction='out')) == {1: 1, 2: 1, 3: 1}
    assert dict(lsa.degree(direction='in')) == {2: 1, 3: 1, 1: 1}
    assert dict(lsa.degree(direction='both')) == {1: 2, 2: 2, 3: 2}
    
    assert set(lsa.neighbors(1, 'out')) == {2}
    assert set(lsa.neighbors(1, 'in')) == {3}
    assert set(lsa.neighbors(1, 'both')) == {2, 3}
    assert list((a, set(b)) for (a, b) in lsa.neighbors(direction='out')) == [(1, {2}), (2, {3}), (3, {1})]
    assert list((a, set(b)) for (a, b) in lsa.neighbors(direction='in')) == [(2, {1}), (3, {2}), (1, {3})]
    assert list((a, set(b)) for (a, b) in lsa.neighbors(direction='both')) == [(1, {2, 3}), (2, {1, 3}), (3, {1, 2})]

    links_b = {(2, 3), (3, 1), (1, 4), (2, 3)}
    lsb = LinkSetDF(links_b, no_duplicates=False)
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


if __name__ == "__main__":
    test_link_set_df()
    test_link_set_weighted()
