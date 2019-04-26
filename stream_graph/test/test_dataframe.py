"""Test file for stream graph."""
from stream_graph.base.interval_df import IntervalDF
from stream_graph.base.interval_df import IntervalWDF
from stream_graph.base.instantaneous_df import InstantaneousDF
from stream_graph.base.instantaneous_df import InstantaneousWDF


def op_(x, cx, y=None, cy=None, oc=1, bk=1, weighted=0, o='u', as_sl=True):
    assert oc >= 0
    oc = [str(i) for i in range(0, oc)]
    twc = ['ts'] + ([] if cx in [InstantaneousDF, InstantaneousWDF] else ['tf']) + (['w'] if cx in [IntervalWDF, InstantaneousWDF] else [])
    x = cx(x, columns=oc+twc)
    kargs = dict(on_column=(oc if len(oc) else []))
    kargs['by_key'] = bool(bk)
    if y is None:
        if o == 'm':
            df = x.merge()
    else:
        cy = (cx if cy is None else cy)
        if bool(bk) and len(oc):
            df = cy(y, columns=oc+twc)
        else:
            df = cy(y, columns=twc)

        if o == 'u':
            df = x.union(df, **kargs)
        elif o == 'i':
            df = x.intersect(df, **kargs)
        elif o == 'd':
            df = x.difference(df, **kargs)

    if as_sl:
        return sorted(list(dump_iter_(df)))
    else:
        return df

def dump_iter_(df):
    if isinstance(df, (InstantaneousWDF, IntervalWDF)):
        return df.itertuples(weights=True)
    else:
        return df.itertuples(name=None, index=False)
    
def test_interval_df():
    cx = IntervalDF
    assert op_([[1, 3, 4], [1, 4, 6]], o='m', cx=cx) == [(1, 3, 6)]
    assert op_([[1, 3, 5], [1, 4, 6]], o='m', cx=cx) == [(1, 3, 6)]
    for bk in [True, False]:
        l = ([1] if bk else [])
        assert op_([[1, 3, 5]], y=[l + [5, 6]], o='u', bk=bk, cx=cx) == [(1, 3, 6)]
        assert op_([[1, 3, 5]], y=[l + [4, 6]], o='u', bk=bk, cx=cx) == [(1, 3, 6)]
        assert op_([[1, 3, 6]], y=[l + [4, 7]], o='i', bk=bk, cx=cx) == [(1, 4, 6)]
        assert op_([[1, 4, 7]], y=[l + [3, 6]], o='i', bk=bk, cx=cx) == [(1, 4, 6)]
        assert op_([[1, 1, 7]], y=[l + [3, 6]], o='i', bk=bk, cx=cx) == [(1, 3, 6)]    
        assert op_([[1, 3, 6]], y=[l + [4, 7]], o='d', bk=bk, cx=cx) == [(1, 3, 4)]
        assert op_([[1, 3, 7]], y=[l + [4, 6]], o='d', bk=bk, cx=cx) == [(1, 3, 4), (1, 6, 7)]
        assert op_([[1, 3, 7]], y=[l + [2, 5]], o='d', bk=bk, cx=cx) == [(1, 5, 7)]
        assert op_([[1, 3, 5]], y=[l + [3, 7]], o='d', bk=bk, cx=cx) == []

def test_interval_wdf():
    cx = IntervalWDF
    assert op_([[1, 3, 5, 1], [1, 4, 6, 2]], o='m', cx=cx) == [(1, 3, 4, 1), (1, 4, 5, 3), (1, 5, 6, 2)]
    for bk in [True, False]:
        l = ([1] if bk else [])
        assert op_([[1, 3, 5, 1]], y=[l + [4, 6, 1]], o='u', bk=bk, cx=cx) == [(1, 3, 4, 1), (1, 4, 5, 2), (1, 5, 6, 1)]
        assert op_([[1, 3, 5, 2], [1, 6, 7, 1]], y=[l + [3, 5, 1], l + [6, 7, 2]], o='u', bk=bk, cx=cx) == [(1, 3, 5, 3.0), (1, 6, 7, 3.0)]
        assert op_([[1, 3, 5, 2], [1, 5, 6, 1]], y=[l + [3, 5, 1], l + [5, 6, 2]], o='u', bk=bk, cx=cx) == [(1, 3, 6, 3.0)]
        assert op_([[1, 3, 6, 1]], y=[l + [4, 7, 1]], o='i', bk=bk, cx=cx) == [(1, 4, 6, 2)]
        assert op_([[1, 4, 7, 1]], y=[l + [3, 6, 1]], o='i', bk=bk, cx=cx) == [(1, 4, 6, 2)]
        assert op_([[1, 1, 7, 1]], y=[l + [3, 6, 1]], o='i', bk=bk, cx=cx) == [(1, 3, 6, 2)]    
        assert op_([[1, 3, 6, 1]], y=[l + [4, 7, 2]], o='d', bk=bk, cx=cx) == [(1, 3, 4, 1)]
        assert op_([[1, 3, 7, 1]], y=[l + [4, 6, 2]], o='d', bk=bk, cx=cx) == [(1, 3, 4, 1), (1, 6, 7, 1)]
        assert op_([[1, 3, 7, 1]], y=[l + [2, 5, 2]], o='d', bk=bk, cx=cx) == [(1, 5, 7, 1)]
        assert op_([[1, 3, 5, 1]], y=[l + [3, 7, 2]], o='d', bk=bk, cx=cx) == []

def test_instantaneous_df():
    cx = InstantaneousDF
    assert op_([[1, 3], [1, 3]], o='m', cx=cx) == [(1, 3)]
    for bk in [True, False]:
        l = ([1] if bk else [])
        assert op_([[1, 3], [1, 4]], y=[l + [4], l + [6]], o='u', bk=bk, cx=cx) == [(1, 3), (1, 4), (1, 6)]
        assert op_([[1, 3], [1, 4]], y=[l + [4], l + [6]], o='i', bk=bk, cx=cx) == [(1, 4)]
        assert op_([[1, 3], [1, 4]], y=[l + [4], l + [6]], o='d', bk=bk, cx=cx) == [(1, 3)]
        assert op_([[1, 4]], y=[l + [4]], o='d', bk=bk, cx=cx) == []

def test_instantaneous_wdf():
    cx = InstantaneousWDF
    assert op_([[1, 3, 2], [1, 3, 1]], o='m', cx=cx) == [(1, 3, 3)]
    for bk in [True, False]:
        l = ([1] if bk else [])
        assert op_([[1, 3, 1], [1, 4, 1]], y=[l + [4, 2], l + [6, 2]], o='u', bk=bk, cx=cx) == [(1, 3, 1), (1, 4, 3), (1, 6, 2)]
        assert op_([[1, 3, 1], [1, 4, 1]], y=[l + [4, 2], l + [6, 2]], o='i', bk=bk, cx=cx) == [(1, 4, 3)]
        assert op_([[1, 3, 1], [1, 4, 1]], y=[l + [4, 2], l + [6, 2]], o='d', bk=bk, cx=cx) == [(1, 3, 1)]
        assert op_([[1, 4, 1]], y=[l + [4, 2]], o='d',  bk=bk, cx=cx) == []

if __name__ == "__main__":
    test_interval_df()
    test_interval_wdf()
    test_instantaneous_df()
    test_instantaneous_wdf()
