"""A file that wraps c++ functions used in stream_graph"""
# Author: Ioannis Siglidis <y.siglidis@gmail.com>

import pandas as pd
import cython

from operator import itemgetter
from numbers import Real
from libcpp.vector cimport vector
from libcpp.pair cimport pair
from stream_graph._c_functions.header cimport closeness, closeness_at
from stream_graph.collections import NodeCollection, TimeCollection

def closeness_c(u, t, df, both, detailed, discrete):
    """C++ wrapped implementation of Cummulative Closeness."""
    assert list(df.columns) == ['u', 'v', 'ts']
    assert df['ts'].dtype.kind in ['i']
    is_interval = (isinstance(t, tuple) and len(t)==2 and all(isinstance(to, int) for to in t) and t[0] < t[1])
    assert is_interval or t is None or t == 'max' or (isinstance(t, Real) or t > .0)

    us = set(df.u.values.flat) | set(df.v.values.flat)
    assert u is None or u in us
    if df['u'].dtype.kind in ['i'] and df['v'].dtype.kind in ['i']:
        def m(a):
            return a
    else:
        # Map nodes to integers
        ud = {a: i for i, a in enumerate(us)}
        def m(a):
            return ud[a]

    # Prepare input
    cdef vector[pair[int, pair[int, int]]] inp
    for a, b, ts in df.itertuples(index=False, name=None):
        inp.push_back(pair[int, pair[int, int]](ts, pair[int, int](m(a), m(b))))

    if is_interval:
        s, f = t
        def iter_(vec):
            return ((t, v) for t, v in vec if t >= s and t <= f)
        t = None
    else:
        def iter_(vec):
            return iter(vec)                    

    if t is 'max':
        def tc(vec, f):
            return max(vec, key=itemgetter(1))[::-1]
        t = None
    else:
        if discrete:
            def tc(vec, f):
                return TimeCollection(iter_(vec), instantaneous=f, discrete=True)
        else:
            def tc(vec, f):
                if f:
                    return TimeCollection(continuous_tc(iter_(vec)), instantaneous=True, discrete=False)
                else:    
                    return TimeCollection(continuous_tc(iter_(vec)), instantaneous=False, discrete=False)

    if u is None:
        if t is None:
            return NodeCollection({u: tc(closeness(inp, m(u), both), detailed) for u in us})
        else:
            return NodeCollection({u: closeness_at(inp, m(u), t, both) for u in us})
    elif t is None:
        return tc(closeness(inp, m(u), both), detailed)
    else:
        return closeness_at(inp, m(u), t, both)

def continuous_tc(vec):
    for t, v in vec:
        yield ((t, True), v)
