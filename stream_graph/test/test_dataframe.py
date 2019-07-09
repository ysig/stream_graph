"""Test file for stream graph."""
from nose.tools import assert_equal
from stream_graph.base.dataframes import CIntervalDF
from stream_graph.base.dataframes import CIntervalWDF
from stream_graph.base.dataframes import DIntervalDF
from stream_graph.base.dataframes import DIntervalWDF
from stream_graph.base.dataframes import InstantaneousDF
from stream_graph.base.dataframes import InstantaneousWDF
from itertools import product


def op_(x, cx, y=None, cy=None, oc=1, bk=1, o='u', as_sl=True):
    assert oc >= 0
    if o in ['ci', 'mi']:
        oc = ['u', 'v']
    else:
        oc = [str(i) for i in range(oc)]
    twc = ['ts']
    if cx in [CIntervalDF, CIntervalWDF]:
        twc += ['tf', 's', 'f']
    elif cx in [DIntervalDF, DIntervalWDF]:
        twc += ['tf']
    if cx in [InstantaneousWDF, DIntervalWDF, CIntervalWDF]:
        twc += ['w']
    x = cx(x, columns=oc+twc)
    kargs = dict(on_column=(oc if len(oc) else []))
    kargs['by_key'] = bool(bk)
    if y is None:
        if o == 'm':
            df = x.merge()
    else:
        cy = (cx if cy is None else cy)
        if o in ['ci', 'mi']:
            df = cy(y, columns=['u']+twc)
        elif bool(bk) and len(oc):
            df = cy(y, columns=oc+twc)
        else:
            df = cy(y, columns=twc)

        if o == 'u':
            df = x.union(df, **kargs)
        elif o == 'i':
            df = x.intersection(df, **kargs)
        elif o == 'd':
            df = x.difference(df, **kargs)
        elif o == 's':
            return x.issuper(df, **kargs)
        elif o == 'nei':
            return x.nonempty_intersection(df, **kargs)
        elif o == 'iis':
            if isinstance(x, (InstantaneousDF, InstantaneousWDF)):
                return x.instants_intersection_size(df)
            else:
                return x.interval_intersection_size(df)
        elif o == 'mi':
            df = x.map_intersection(df)
        elif o == 'ci':
            df = x.cartesian_intersection(df)

    if as_sl:
        return sorted(list(dump_iter_(df)))
    else:
        return df


def dump_iter_(df):
    if isinstance(df, CIntervalDF):
        return df.itertuples(bounds=True)
    elif isinstance(df, CIntervalWDF):
        return df.itertuples(bounds=True, weights=True)
    elif isinstance(df, (InstantaneousDF, DIntervalDF)):
        return df.itertuples()
    elif isinstance(df, (DIntervalWDF, InstantaneousWDF)):
        return df.itertuples(weights=True)
    else:
        return df.itertuples(name=None, index=False)


def test_cinterval_df():
    cx = CIntervalDF
    for l in [[], [1]]:
        for a, b, c, d in product([False, True], [False, True], [False, True], [False, True]):
            if c or d:
                assert_equal(op_([l + [3, 4, a, c], l + [4, 6, d, b]], o='m', cx=cx, oc=len(l)), [tuple(l) + (3, 6, a, b)])
            else:
                assert_equal(op_([l + [3, 4, a, False], l + [4, 6, False, b]], o='m', cx=cx, oc=len(l)), [tuple(l) + (3, 4, a, False), tuple(l) + (4, 6, False, b)])
            assert_equal(op_([l + [3, 5, a, c], l + [4, 6, d, b]], o='m', cx=cx, oc=len(l)), [tuple(l) + (3, 6, a, b)])

    for k in [[], [1]]:
        for bk in [True, False]:
            l = (k if bk and len(k) else [])
            for a, b, c, d in product([False, True], [False, True], [False, True], [False, True]):
                # Union
                if c or d:
                    assert_equal(op_([k + [3, 4, a, c]], y=[l + [4, 6, d, b]], o='u', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (3, 6, a, b)])
                else:
                    assert_equal(op_([k + [3, 4, a, False]], y=[l + [4, 6, False, b]], o='u', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (3, 4, a, False), tuple(k) + (4, 6, False, b)])
                # Intersection
                assert_equal(op_([k + [3, 6, a, b]], y=[l + [4, 7, c, d]], o='i', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (4, 6, c, b)])
                assert_equal(op_([k + [4, 7, a, b]], y=[l + [3, 6, c, d]], o='i', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (4, 6, a, d)])
                assert_equal(op_([k + [1, 7, a, b]], y=[l + [3, 6, c, d]], o='i', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (3, 6, c, d)])
                assert_equal(op_([k + [1, 7, a, b]], y=[l + [1, 7, c, d]], o='i', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (1, 7, a and c, b and d)])
                # Difference
                assert_equal(op_([k + [3, 6, a, b]], y=[l + [4, 7, c, d]], o='d', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (3, 4, a, not c)])
                assert_equal(op_([k + [3, 7, a, b]], y=[l + [4, 6, c, d]], o='d', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (3, 4, a, not c), tuple(k) + (6, 7, not d, b)])
                assert_equal(op_([k + [3, 7, a, b]], y=[l + [2, 5, c, d]], o='d', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (5, 7, not d, b)])
                if a > c:
                    assert_equal(op_([k + [3, 5, a, b]], y=[l + [3, 7, c, d]], o='d', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (3, 3, True, True)])
                else:
                    assert_equal(op_([k + [3, 5, a, b]], y=[l + [3, 7, c, d]], o='d', bk=bk, cx=cx, oc=len(k)), [])
                tv = []
                if a > c:
                    tv.append(tuple(k) + (3, 3, True, True))
                if b > d:
                    tv.append(tuple(k) + (7, 7, True, True))
                # Issuper
                assert_equal(op_([k + [3, 7, a, b]], y=[l + [3, 7, c, d]], o='s', bk=bk, cx=cx, oc=len(k)), not (a < c or b < d))
                if a:
                    assert_equal(op_([k + [3, 7, True, b]], y=[l + [3, 3, True, True], l + [4, 7, c, d]], o='s', bk=bk, cx=cx, oc=len(k)), not b < d)
                assert_equal(op_([k + [3, 8, a, b]], y=[l + [5, 8, c, d]], o='s', bk=bk, cx=cx, oc=len(k)), not d > b)
                assert_equal(op_([k + [3, 8, a, b]], y=[l + [3, 5, c, d]], o='s', bk=bk, cx=cx, oc=len(k)), not c > a)
                assert_equal(op_([k + [3, 8, a, b]], y=[l + [4, 5, c, d]], o='s', bk=bk, cx=cx, oc=len(k)), True)
                assert_equal(op_([k + [3, 8, a, b]], y=[l + [1, 10, c, d]], o='s', bk=bk, cx=cx, oc=len(k)), False)
                # Non-empty Intersection
                assert_equal(op_([k + [5, 8, a, b]], y=[l + [3, 6, c, d]], o='nei', bk=bk, cx=cx, oc=len(k)), True)
                assert_equal(op_([k + [3, 8, a, b]], y=[l + [8, 10, c, d]], o='nei', bk=bk, cx=cx, oc=len(k)), b and c)
                assert_equal(op_([k + [3, 8, a, b]], y=[l + [4, 5, c, d]], o='nei', bk=bk, cx=cx, oc=len(k)), True)

    assert_equal(op_([[2, 1, 1, 3, True, False], [1, 2, 2, 5, True, False], [2, 1, 6, 8, True, False]], y=[[1, 1, 4, True, True], [2, 1, 2, True, True]], o='ci', cx=cx, oc=len(k)), [(1, 2, 2, 2, True, True), (2, 1, 1, 2, True, True)])
    for a, b, c, d in product([False, True], [False, True], [False, True], [False, True]):
        # Cartesian Intersection
        assert_equal(op_([[1, 2, 1, 10, False, False]], y=[[1, 2, 5, a, b], [2, 3, 7, c, d]], o='ci', cx=cx, oc=len(k)), [(1, 2, 3, 5, c, b)])
        # Map Intersection
        assert_equal(op_([[1, 2, 1, 5, a, b]], y=[[1, 2, 10, c, d]], o='mi', cx=cx, oc=len(k)), [(2, 2, 5, c, b)])

    # Interval Intersection Size
    assert_equal(op_([[1, 4, 8, False, True]], y=[[1, 8, 10, True, True]], o='iis', cx=cx, oc=len(k)), 0)
    assert_equal(op_([[1, 4, 8, False, True]], y=[[1, 9, 10, False, True]], o='iis', cx=cx, oc=len(k)), 0)
    assert_equal(op_([[1, 3, 6, True, False], [1, 6, 9, False, False]], y=[[1, 2, 5, False, True], [2, 4, 8, True, True], [1, 5, 8, False, True]], o='iis', cx=cx, oc=len(k)), 9)


def test_dinterval_df():
    cx = DIntervalDF
    for k in [[], [1]]:
        assert_equal(op_([k + [3, 4], k + [4, 6]], o='m', cx=cx, oc=len(k)), [tuple(k) + (3, 6)])
        assert_equal(op_([k + [3, 4], k + [5, 6]], o='m', cx=cx, oc=len(k)), [tuple(k) + (3, 6)])
        assert_equal(op_([k + [3, 5], k + [4, 6]], o='m', cx=cx, oc=len(k)), [tuple(k) + (3, 6)])
        assert_equal(op_([k + [2, 3], k + [5, 6]], o='m', cx=cx, oc=len(k)), [tuple(k) + (2, 3), tuple(k) + (5, 6)])
        for bk in [True, False]:
            l = (k if bk else [])
            # Union
            assert_equal(op_([k + [3, 4]], y=[l + [4, 6]], o='u', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (3, 6)])
            assert_equal(op_([k + [3, 4]], y=[l + [5, 6]], o='u', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (3, 6)])
            assert_equal(op_([k + [3, 5]], y=[l + [4, 6]], o='u', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (3, 6)])
            assert_equal(op_([k + [2, 3]], y=[l + [5, 6]], o='u', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (2, 3), tuple(k) + (5, 6)])
            # Intersection
            assert_equal(op_([k + [3, 6]], y=[l + [4, 7]], o='i', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (4, 6)])
            assert_equal(op_([k + [4, 7]], y=[l + [3, 6]], o='i', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (4, 6)])
            assert_equal(op_([k + [1, 7]], y=[l + [3, 6]], o='i', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (3, 6)])
            assert_equal(op_([k + [1, 7]], y=[l + [1, 7]], o='i', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (1, 7)])
            # Difference
            assert_equal(op_([k + [3, 6]], y=[l + [4, 7]], o='d', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (3, 3)])
            assert_equal(op_([k + [3, 7]], y=[l + [4, 6]], o='d', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (3, 3), tuple(k) + (7, 7)])
            assert_equal(op_([k + [3, 7]], y=[l + [2, 5]], o='d', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (6, 7)])
            assert_equal(op_([k + [3, 7]], y=[l + [3, 7]], o='d', bk=bk, cx=cx, oc=len(k)), [])
            assert_equal(op_([k + [3, 5]], y=[l + [3, 7]], o='d', bk=bk, cx=cx, oc=len(k)), [])
            # Issuper
            assert_equal(op_([k + [3, 7]], y=[l + [3, 7]], o='s', bk=bk, cx=cx, oc=len(k)), True)
            assert_equal(op_([k + [3, 7]], y=[l + [3, 6]], o='s', bk=bk, cx=cx, oc=len(k)), True)
            assert_equal(op_([k + [3, 7]], y=[l + [4, 7]], o='s', bk=bk, cx=cx, oc=len(k)), True)
            assert_equal(op_([k + [3, 8]], y=[l + [1, 10]], o='s', bk=bk, cx=cx, oc=len(k)), False)
            assert_equal(op_([k + [2, 6]], y=[l + [3, 7]], o='s', bk=bk, cx=cx, oc=len(k)), False)
            assert_equal(op_([k + [4, 8]], y=[l + [3, 7]], o='s', bk=bk, cx=cx, oc=len(k)), False)
            # Non-empty-intersection
            assert_equal(op_([k + [5, 8]], y=[l + [3, 6]], o='nei', bk=bk, cx=cx, oc=len(k)), True)
            assert_equal(op_([k + [3, 8]], y=[l + [8, 10]], o='nei', bk=bk, cx=cx, oc=len(k)), True)
            assert_equal(op_([k + [3, 8]], y=[l + [4, 5]], o='nei', bk=bk, cx=cx, oc=len(k)), True)
            assert_equal(op_([k + [3, 8]], y=[l + [9, 10]], o='nei', bk=bk, cx=cx, oc=len(k)), False)

    # Cartesian Intersection
    assert_equal(op_([[1, 2, 1, 10]], y=[[1, 2, 5], [2, 3, 7]], o='ci', cx=cx, oc=len(k)), [(1, 2, 3, 5)])
    # Map Intersection
    assert_equal(op_([[1, 2, 1, 5]], y=[[1, 2, 10]], o='mi', cx=cx, oc=len(k)), [(2, 2, 5)])

    # Interval Intersection
    assert_equal(op_([[1, 4, 8]], y=[[1, 8, 10]], o='iis', cx=cx, oc=len(k)), 1)
    assert_equal(op_([[1, 4, 8]], y=[[1, 9, 10]], o='iis', cx=cx, oc=len(k)), 0)
    assert_equal(op_([[1, 3, 9], [2, 10, 11]], y=[[1, 2, 5], [2, 4, 8], [1, 7, 11]], o='iis', cx=cx, oc=len(k)), 13)


def test_dinterval_wdf():
    cx = DIntervalWDF
    for k in [[], [1]]:
        assert_equal(op_([k + [3, 5, 1], k + [5, 6, 2]], o='m', cx=cx, oc=len(k)), [tuple(k) + (3, 4, 1), tuple(k) + (5, 5, 3), tuple(k) + (6, 6, 2)])
        for bk in [True, False]:
            l = (k if bk else [])
            # Union
            assert_equal(op_([k + [3, 5, 1]], y=[l + [5, 6, 1]], o='u', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (3, 4, 1), tuple(k) + (5, 5, 2), tuple(k) + (6, 6, 1)])
            assert_equal(op_([k + [3, 5, 2], k + [6, 7, 1]], y=[l + [3, 5, 1], l + [6, 7, 2]], o='u', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (3, 7, 3.0)])
            # Intersection
            assert_equal(op_([k + [3, 6, 1]], y=[l + [4, 7, 4]], o='i', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (4, 6, 4)])
            assert_equal(op_([k + [4, 7, 1]], y=[l + [3, 6, 3]], o='i', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (4, 6, 3)])
            assert_equal(op_([k + [1, 7, 2]], y=[l + [3, 6, 1]], o='i', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (3, 6, 2)])
            # Difference
            assert_equal(op_([k + [3, 6, 3]], y=[l + [4, 7, 1]], o='d', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (3, 3, 3), tuple(k) + (4, 6, 2)])
            assert_equal(op_([k + [3, 7, 1]], y=[l + [4, 6, 1]], o='d', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (3, 3, 1), tuple(k) + (7, 7, 1)])
            assert_equal(op_([k + [3, 7, 1]], y=[l + [2, 5, 1]], o='d', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (6, 7, 1)])
            assert_equal(op_([k + [3, 5, 1]], y=[l + [3, 7, 1]], o='d', bk=bk, cx=cx, oc=len(k)), [])
            # Issuper
            assert_equal(op_([k + [3, 7, 1]], y=[l + [3, 7, 1]], o='s', bk=bk, cx=cx, oc=len(k)), True)
            assert_equal(op_([k + [3, 7, 1]], y=[l + [3, 6, 1]], o='s', bk=bk, cx=cx, oc=len(k)), True)
            assert_equal(op_([k + [3, 7, 1]], y=[l + [4, 7, 1]], o='s', bk=bk, cx=cx, oc=len(k)), True)
            assert_equal(op_([k + [3, 8, 1]], y=[l + [1, 10, 1]], o='s', bk=bk, cx=cx, oc=len(k)), False)
            assert_equal(op_([k + [2, 6, 1]], y=[l + [3, 7, 1]], o='s', bk=bk, cx=cx, oc=len(k)), False)
            assert_equal(op_([k + [4, 8, 1]], y=[l + [3, 7, 1]], o='s', bk=bk, cx=cx, oc=len(k)), False)
            assert_equal(op_([k + [3, 7, 1]], y=[l + [3, 7, 2]], o='s', bk=bk, cx=cx, oc=len(k)), False)
            # Non-empty-intersection
            assert_equal(op_([k + [5, 8, 1]], y=[l + [3, 6, 1]], o='nei', bk=bk, cx=cx, oc=len(k)), True)
            assert_equal(op_([k + [3, 8, 1]], y=[l + [8, 10, 1]], o='nei', bk=bk, cx=cx, oc=len(k)), True)
            assert_equal(op_([k + [3, 8, 1]], y=[l + [4, 5, 1]], o='nei', bk=bk, cx=cx, oc=len(k)), True)
            assert_equal(op_([k + [3, 8, 1]], y=[l + [9, 10, 1]], o='nei', bk=bk, cx=cx, oc=len(k)), False)


    # Cartesian Intersection
    assert_equal(op_([[1, 2, 1, 10, 2]], y=[[1, 2, 5, 1], [2, 3, 7, 6]], o='ci', cx=cx, oc=len(k)), [(1, 2, 3, 5, 6)])

    # Interval Intersection
    assert_equal(op_([[1, 4, 8, 5]], y=[[1, 8, 10, 1]], o='iis', cx=cx, oc=len(k)), 1)
    assert_equal(op_([[1, 4, 8, 8]], y=[[1, 9, 10, 10]], o='iis', cx=cx, oc=len(k)), 0)
    assert_equal(op_([[1, 3, 9, 1], [2, 10, 11, 2]], y=[[1, 2, 5, 3], [2, 4, 8, 4], [1, 7, 11, 6]], o='iis', cx=cx, oc=len(k)), 9)


def test_cinterval_wdf():
    cx = CIntervalWDF
    for l in [[], [1]]:
        for a, b, c, d in product([False, True], [False, True], [False, True], [False, True]):
            assert_equal(op_([l + [3, 5, a, c, 1], l + [4, 6, d, b, 1]], o='m', cx=cx, oc=len(l)), [tuple(l) + (3, 4, a, not d, 1), tuple(l) + (4, 5, d, c, 2), tuple(l) + (5, 6, not c, b, 1)])
            if c and d:
                assert_equal(op_([l + [3, 4, a, c, 1], l + [4, 6, d, b, 1]], o='m', cx=cx, oc=len(l)), [tuple(l) + (3, 4, a, False, 1), tuple(l) + (4, 4, True, True, 2), tuple(l) + (4, 6, False, b, 1)])
            elif c or d:
                assert_equal(op_([l + [3, 4, a, c, 1], l + [4, 6, d, b, 1]], o='m', cx=cx, oc=len(l)), [tuple(l) + (3, 6, a, b, 1)])
            else:
                assert_equal(op_([l + [3, 4, a, False, 1], l + [4, 6, False, b, 1]], o='m', cx=cx, oc=len(l)), [tuple(l) + (3, 4, a, False, 1), tuple(l) + (4, 6, False, b, 1)])

    for k in [[], [1]]:
        for bk in [True, False]:
            l = (k if bk and len(k) else [])
            for a, b, c, d in product([False, True], [False, True], [False, True], [False, True]):
                # Union
                if c and d:
                    assert_equal(op_([k + [3, 4, a, c, 1]], y=[l + [4, 6, d, b, 1]], o='u', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (3, 4, a, False, 1), tuple(k) + (4, 4, True, True, 2), tuple(k) + (4, 6, False, b, 1)])
                elif c or d:
                    assert_equal(op_([k + [3, 4, a, c, 1]], y=[l + [4, 6, d, b, 1]], o='u', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (3, 6, a, b, 1)])
                else:
                    assert_equal(op_([k + [3, 4, a, False, 1]], y=[l + [4, 6, False, b, 1]], o='u', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (3, 4, a, False, 1), tuple(k) + (4, 6, False, b, 1)])
                # Intersection
                assert_equal(op_([k + [3, 6, a, b, 2]], y=[l + [4, 7, c, d, 3]], o='i', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (4, 6, c, b, 3)])
                assert_equal(op_([k + [4, 7, a, b, 1]], y=[l + [3, 6, c, d, 3]], o='i', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (4, 6, a, d, 3)])
                assert_equal(op_([k + [1, 7, a, b, 3]], y=[l + [3, 6, c, d, 2]], o='i', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (3, 6, c, d, 3)])
                assert_equal(op_([k + [1, 7, a, b, 1]], y=[l + [1, 7, c, d, 3]], o='i', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (1, 7, a and c, b and d, 3)])
                # Difference
                assert_equal(op_([k + [3, 6, a, b, 3]], y=[l + [4, 7, c, d, 2]], o='d', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (3, 4, a, not c, 3), tuple(k) + (4, 6, c, b, 1)])
                for e, f in product([False, True], [False, True]):
                    assert_equal(op_([k + [3, 8, a, b, 3]], y=[l + [4, 5, c, e, 2], l + [5, 6, not e, not f, 3], l + [6, 7, f, d, 2]], o='d', bk=bk, cx=cx, oc=len(k)),
                                 [tuple(k) + (3, 4, a, not c, 3), tuple(k) + (4, 5, c, e, 1), tuple(k) + (6, 7, f, d, 1), tuple(k) + (7, 8, not d, b, 3)])
                assert_equal(op_([k + [3, 7, a, b, 1]], y=[l + [2, 5, c, d, 1]], o='d', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (5, 7, not d, b, 1)])
                if a > c:
                    assert_equal(op_([k + [3, 5, a, b, 1]], y=[l + [3, 7, c, d, 1]], o='d', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (3, 3, True, True, 1)])
                else:
                    assert_equal(op_([k + [3, 5, a, b, 1]], y=[l + [3, 7, c, d, 1]], o='d', bk=bk, cx=cx, oc=len(k)), [])
                tv = []
                if a > c:
                    tv.append(tuple(k) + (3, 3, True, True, 1))
                if b > d:
                    tv.append(tuple(k) + (7, 7, True, True, 1))
                assert_equal(op_([k + [3, 7, a, b, 1]], y=[l + [3, 7, c, d, 1]], o='d', bk=bk, cx=cx, oc=len(k)), tv)
                # Issuper
                assert_equal(op_([k + [3, 7, a, b, 1]], y=[l + [3, 7, c, d, 1]], o='s', bk=bk, cx=cx, oc=len(k)), not (a < c or b < d))
                assert_equal(op_([k + [3, 8, a, b, 1]], y=[l + [5, 8, c, d, 1]], o='s', bk=bk, cx=cx, oc=len(k)), not d > b)
                assert_equal(op_([k + [3, 8, a, b, 1]], y=[l + [3, 5, c, d, 1]], o='s', bk=bk, cx=cx, oc=len(k)), not c > a)
                assert_equal(op_([k + [3, 8, a, b, 1]], y=[l + [4, 5, c, d, 1]], o='s', bk=bk, cx=cx, oc=len(k)), True)
                assert_equal(op_([k + [3, 8, a, b, 1]], y=[l + [1, 10, c, d, 1]], o='s', bk=bk, cx=cx, oc=len(k)), False)
                assert_equal(op_([k + [3, 8, a, b, 1]], y=[l + [4, 6, c, d, 2]], o='s', bk=bk, cx=cx, oc=len(k)), False)
                # Non-empty Intersection
                assert_equal(op_([k + [5, 8, a, b, 1]], y=[l + [3, 6, c, d, 1]], o='nei', bk=bk, cx=cx, oc=len(k)), True)
                assert_equal(op_([k + [3, 8, a, b, 1]], y=[l + [8, 10, c, d, 1]], o='nei', bk=bk, cx=cx, oc=len(k)), b and c)
                assert_equal(op_([k + [3, 8, a, b, 1]], y=[l + [4, 5, c, d, 1]], o='nei', bk=bk, cx=cx, oc=len(k)), True)

    for a, b, c, d in product([False, True], [False, True], [False, True], [False, True]):
        # Cartesian Intersection
        assert_equal(op_([[1, 2, 1, 10, False, False, 1]], y=[[1, 2, 5, a, b, 3], [2, 3, 7, c, d, 2]], o='ci', cx=cx, oc=len(k)), [(1, 2, 3, 5, c, b, 3)])

    # Interval Intersection Size
    assert_equal(op_([[1, 4, 8, False, True, 2]], y=[[1, 8, 10, True, True, 1]], o='iis', cx=cx, oc=len(k)), 0)
    assert_equal(op_([[1, 4, 8, False, True, 2]], y=[[1, 9, 10, False, True, 1]], o='iis', cx=cx, oc=len(k)), 0)
    assert_equal(op_([[1, 3, 6, True, False, 2], [1, 6, 9, False, False, 8]], y=[[1, 2, 5, False, True, 5], [2, 4, 8, True, True, 4], [1, 5, 8, False, True, 7]], o='iis', cx=cx, oc=len(k)), 20)


def test_instantaneous_df():
    cx = InstantaneousDF
    for k in [[], [1]]:
        assert_equal(op_([k + [3], k + [3]], o='m', cx=cx, oc=len(k)), [tuple(k) + (3,)])
        for bk in [True, False]:
            l = (k if bk else [])
            assert_equal(op_([k + [3], k + [4]], y=[l + [4], l + [6]], o='u', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (3,), tuple(k) + (4,), tuple(k) + (6,)])
            assert_equal(op_([k + [3], k + [4]], y=[l + [4], l + [6]], o='i', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (4,)])
            assert_equal(op_([k + [3], k + [4]], y=[l + [4], l + [6]], o='d', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (3,)])
            assert_equal(op_([k + [4]], y=[l + [4]], o='d', bk=bk, cx=cx, oc=len(k)), [])
            assert_equal(op_([k + [a] for a in [1, 2, 3, 4]], y=[l + [a] for a in [1, 2, 3, 4]], o='s', bk=bk, cx=cx, oc=len(k)), True)
            assert_equal(op_([k + [a] for a in [1, 2, 3, 4]], y=[l + [a] for a in [1, 2, 5]], o='s', bk=bk, cx=cx, oc=len(k)), False)
            assert_equal(op_([k + [a] for a in [1, 2, 3, 4]], y=[l + [a] for a in [1, 6, 7]], o='nei', bk=bk, cx=cx, oc=len(k)), True)
            assert_equal(op_([k + [a] for a in [1, 2, 3, 4]], y=[l + [a] for a in [6, 7]], o='nei', bk=bk, cx=cx, oc=len(k)), False)

    # Cartesian Intersection
    assert_equal(op_([[1, 2] + [a] for a in [1, 2, 3, 4]], y=[[1, 2], [1, 3], [2, 3], [2, 4]], o='ci', cx=cx, oc=len(k)), [(1, 2, 3)])
    assert_equal(op_([[1, 2, a] for a in [1, 2, 3, 4]], y=[[1, 2], [1, 3]], o='mi', cx=cx, oc=len(k)), [(2, 2), (2, 3)])

    # Interval Intersection Size
    assert_equal(op_([k + [a] for a in [1, 2, 3, 4]], y=[l + [a] for a in [6, 7]], o='iis', bk=bk, cx=cx, oc=len(k)), 0)
    assert_equal(op_([[1] + [a] for a in [3, 4, 5, 6]] + [[2] + [a] for a in [1, 2, 3]], y=[[1] + [a] for a in [1, 2, 3, 4]] + [[2] + [a] for a in [3, 4, 6]], o='iis', cx=cx, oc=len(k)), 9)


def test_instantaneous_wdf():
    cx = InstantaneousWDF
    for k in [[], [1]]:
        assert_equal(op_([k + [3, 2], k + [3, 1]], o='m', cx=cx, oc=len(k)), [tuple(k) + (3, 3)])
        for bk in [True, False]:
            l = (k if bk else [])
            assert_equal(op_([k + [3, 1], k + [4, 1]], y=[l + [4, 2], l + [6, 2]], o='u', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (3, 1), tuple(k) + (4, 3), tuple(k) + (6, 2)])
            assert_equal(op_([k + [3, 1], k + [4, 1]], y=[l + [4, 3], l + [6, 2]], o='i', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (4, 3)])
            assert_equal(op_([k + [3, 1], k + [4, 1]], y=[l + [4, 2], l + [6, 2]], o='d', bk=bk, cx=cx, oc=len(k)), [tuple(k) + (3, 1)])
            assert_equal(op_([k + [4, 1]], y=[l + [4, 2]], o='d',  bk=bk, cx=cx, oc=len(k)), [])
            assert_equal(op_([k + [1, 1], k + [2, 1], k + [3, 1]], y=[l + [1, 1], l + [2, 1], l + [3, 1]], o='s',  bk=bk, cx=cx, oc=len(k)), True)
            assert_equal(op_([k + [1, 1], k + [2, 1], k + [3, 1], k + [4, 1]], y=[l + [2, 1], l + [5, 1], l + [4, 2]], o='s',  bk=bk, cx=cx, oc=len(k)), False)
            assert_equal(op_([k + [1, 1], k + [2, 1], k + [3, 1], k + [4, 1]], y=[l + [2, 1], l + [3, 2]], o='s',  bk=bk, cx=cx, oc=len(k)), False)
            assert_equal(op_([k + [a, 1] for a in [1, 2, 3, 4]], y=[l + [a, 1] for a in [1, 6, 7]], o='nei', bk=bk, cx=cx, oc=len(k)), True)
            assert_equal(op_([k + [a, 1] for a in [1, 2, 3, 4]], y=[l + [a, 1] for a in [6, 7]], o='nei', bk=bk, cx=cx, oc=len(k)), False)
            assert_equal(op_([k + [a, 2] for a in [1, 2, 3, 4]], y=[l + [a, 1] for a in [1, 6, 7]], o='nei', bk=bk, cx=cx, oc=len(k)), True)
            assert_equal(op_([k + [a, 1] for a in [1, 2, 3, 4]], y=[l + [a, 2] for a in [1, 6, 7]], o='nei', bk=bk, cx=cx, oc=len(k)), False)

    # Cartesian Intersection
    assert_equal(op_([[1, 2] + [a, 2] for a in [1, 2, 3, 4]], y=[[1, 2, 1], [1, 3, 3], [2, 3, 4], [2, 4, 6]], o='ci', cx=cx, oc=len(k)), [(1, 2, 3, 4)])

    # Interval Intersection Size
    assert_equal(op_([k + [a, 1] for a in [1, 2, 3, 4]], y=[l + [a, 2] for a in [6, 7]], o='iis', bk=bk, cx=cx, oc=len(k)), 0)
    assert_equal(op_([[1] + a for a in [[3, 2], [4, 1], [5, 2], [6, 3]]] + [[2] + a for a in [[1, 3], [2, 1], [3, 2]]], y=[[1] + a for a in [[1, 3], [2, 2], [3, 1], [4, 5]]] + [[2] + a for a in [[3, 2], [4, 3], [6, 2]]], o='iis', cx=cx, oc=len(k)), 10)


def set_nodes_weighted(cache, i):
    if i[0]:
        cache[i[2][0]] += 1
    else:
        cache[i[2][0]] -= 1
        if cache[i[2][0]] == 0:
            cache.pop(i[2][0], None)
    return set(cache.keys())


def sum_nodes_weighted(cache, i):
    if i[0]:
        cache[i[2][0]] += i[3]
    else:
        cache[i[2][0]] -= i[3]
        if cache[i[2][0]] == 0:
            cache.pop(i[2][0], None)
    return sum(itervalues(cache[i[2][0]]))


def test_time_generators_builders():
    from collections import Counter
    from stream_graph.base.multi_df_utils import build_time_generator, set_nodes, set_nodes_sparse, sum_counter_

    df = CIntervalDF([(1, 1, 3, True, False), (1, 4, 6, False, False), (2, 2, 3, False, True), (2, 5, 7, True, True)], columns=['u', 'ts', 'tf', 's', 'f'])
    assert_equal(list(build_time_generator(df, set, set_nodes)), [((1, True), {1}), ((2, False), {1, 2}), ((3, True), {2}), ((3, False), set()), ((4, False), {1}), ((5, True), {1, 2}), ((6, True), {2}), ((7, False), set())])
    assert_equal(list(build_time_generator(df, set, set_nodes_sparse, sparse=True)), [((1, True), {1}, True), ((2, False), {2}, True), ((3, True), {1}, False), ((3, False), {2}, False), ((4, False), {1}, True), ((5, True), {2}, True), ((6, True), {1}, False), ((7, False), {2}, False)])

    df = DIntervalDF([(1, 1, 3), (1, 5, 6), (2, 2, 2), (2, 4, 7)], columns=['u', 'ts', 'tf'])
    assert_equal(list(build_time_generator(df, set, set_nodes)), [(1, {1}), (2, {1, 2}), (3, {1}), (4, {2}), (5, {1, 2}), (7, {2}), (8, set())])
    assert_equal(list(build_time_generator(df, set, set_nodes_sparse, sparse=True)), [(1, {1}, True), (2, {2}, True), (3, {2}, False), (4, {1}, False), (4, {2}, True), (5, {1}, True), (7, {1}, False), (8, {2}, False)])

    df = CIntervalWDF([(1, 1, 3, True, False, 1), (1, 3, 6, True, False, 2), (2, 2, 3, False, True, 2), (2, 5, 7, True, True, 1)], columns=['u', 'ts', 'tf', 's', 'f', 'w'])
    assert_equal(list(build_time_generator(df, Counter, set_nodes_weighted)), [((1, True), {1}), ((2, False), {1, 2}), ((3, False), {1}), ((5, True), {1, 2}), ((6, True), {2}), ((7, False), set())])
    assert_equal(list(build_time_generator(df, set, set_nodes_sparse, sparse=True)), [((1, True), {1}, True), ((2, False), {2}, True), ((3, True), {1}, False), ((3, True), {1}, True), ((3, False), {2}, False), ((5, True), {2}, True), ((6, True), {1}, False), ((7, False), {2}, False)])
    assert_equal(list(build_time_generator(df, Counter, sum_counter_)), [((1, True), 1), ((2, False), 3), ((3, True), 4), ((3, False), 2), ((5, True), 3), ((6, True), 1), ((7, False), 0)])

    df = CIntervalWDF([(1, 1, 3, True, False, 1), (1, 3, 6, True, False, 2), (2, 2, 3, True, False, 2), (2, 3, 5, True, True, 1)], columns=['u', 'ts', 'tf', 's', 'f', 'w'])
    assert_equal(list(build_time_generator(df, Counter, set_nodes_weighted)), [((1, True), {1}), ((2, True), {1, 2}), ((5, False), {1}), ((6, True), set())])
    assert_equal(list(build_time_generator(df, set, set_nodes_sparse, sparse=True)), [((1, True), {1}, True), ((2, True), {2}, True), ((3, True), {1}, False), ((3, True), {2}, False), ((3, True), {2}, True), ((5, False), {2}, False), ((6, True), {1}, False)])
    assert_equal(list(build_time_generator(df, Counter, sum_counter_)), [((1, True), 1), ((2, True), 3), ((5, False), 2), ((6, True), 0)])

    df = DIntervalWDF([(1, 1, 2, 1), (1, 4, 6, 2), (2, 2, 3, 2), (2, 5, 7, 1)], columns=['u', 'ts', 'tf', 'w'])
    assert_equal(list(build_time_generator(df, set, set_nodes)), [(1, {1}), (2, {1, 2}), (3, {2}), (4, {1}), (5, {1, 2}), (7, {2}), (8, set())])
    assert_equal(list(build_time_generator(df, set, set_nodes_sparse, sparse=True)), [(1, {1}, True), (2, {2}, True), (3, {1}, False), (4, {2}, False), (4, {1}, True), (5, {2}, True), (7, {1}, False), (8, {2}, False)])
    assert_equal(list(build_time_generator(df, Counter, sum_counter_)), [(1, 1), (2, 3), (3, 2), (5, 3), (7, 1), (8, 0)])



if __name__ == "__main__":
    """
    test_cinterval_df()
    test_cinterval_wdf()
    test_dinterval_df()
    test_dinterval_wdf()
    test_instantaneous_df()
    test_instantaneous_wdf()
    """
    test_time_generators_builders()
