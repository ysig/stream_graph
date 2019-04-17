"""A file that wraps c++ functions used in stream_graph"""
# Author: Ioannis Siglidis <y.siglidis@gmail.com>

import pandas as pd
import cython

from libcpp.vector cimport vector
from libcpp.pair cimport pair
from stream_graph._c_functions.header cimport closeness
from stream_graph.collections import NodeCollection

def ego(u, df, both):
    """C++ wrapped implementation of Cummulative Closeness."""
    assert list(df.columns) == ['u', 'v', 'ts']
    assert df['ts'].dtype.kind in ['i']

    us = set(df.u.values.flat) | set(df.v.values.flat)
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

    if u is None:
        return NodeCollection({u: closeness(inp, m(u)) for u in us})
    else:
        return closeness(inp, m(u))

