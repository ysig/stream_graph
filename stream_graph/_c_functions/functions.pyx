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

def closeness_c(u, t, df, both, detailed):
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
        def tc(vec):
            return max(vec, key=itemgetter(1))[::-1]
        t = None
    elif detailed:
        def tc(vec):
            return TimeCollection(iter_(vec), True)
    else:
        def tc(vec):
            def iterate(vec):
                prev = None
                for t, v in iter_(vec):
                    if v == prev:
                        continue
                    yield (t, v)
                    prev = v
            return TimeCollection(iterate(vec))

    if u is None:
        if t is None:
            return NodeCollection({u: tc(closeness(inp, m(u), both)) for u in us})
        else:
            return NodeCollection({u: closeness_at(inp, m(u), t, both) for u in us})
    elif t is None:
        return tc(closeness(inp, m(u), both))
    else:
        return closeness_at(inp, m(u), t, both)


def ego(e, ne, l, both, detailed):
    # print >> sys.stderr, "Running node : " + str(e)
    u, v, t, index, times = 0, 0, 0, 0, list()

    ce, info = dict(), dict()
    for i in ne:
        info[(e,i)] = -1
        for x in ne-{i}:
            info[(i,x)] = -1
        info[(i,e)] = -1

    index = 0
    time = l[index][2] #starting time.
    ne_x, lines, paths = ne | {e}, dict(), dict()

    if both:
        def add_lines(u, v, t):
            lines[(u,v)] = t
            lines[(v,u)] = t 
    else:
        def add_lines(u, v, t):
            lines[(u,v)] = t

    if detailed:
        def take(times, prev, val):
            times.append((time, val))
    else:
        def take(times, prev, val):
            if prev[0] is None or prev[0] != val:
                times.append((time, val))
                prev[0] = val

    prev = [None]
    while(index < len(l) -1):
        # get all links of time stamp
        while(index < len(l) -1):
            u,v,t = l[index]
            add_lines(u, v, t)
            index += 1
            if(t != time):
                break

        for u in ne:
            for v in ne-{u}:
                # print u,v
                ce[(u,v)] = 0.0
                Q = set()
                if (u,v) not in lines:
                    # print u,v
                    news = info[(u,v)]
                    for x in ne_x - {u, v}:
                        ux = info[(u, x)]
                        if (x, v) in lines:
                            xv = lines[(x, v)]
                        else:
                            xv = info[(x, v)]
                        if ux != -1 and xv != -1 and ux < xv: 
                            if ux == news:
                                Q.add(x)
                            if ux > news:
                                Q = {x}
                                news = ux

                    if (u,v) in paths:
                        old_paths = paths[(u,v)]
                        if old_paths[0] == news:
                            paths[(u, v)] = (news, paths[(u,v)][1] | Q)
                        elif old_paths[0] < news:
                            paths[(u, v)] = (news, Q)
                    else:
                        paths[(u, v)] = (news, Q)

                    if e in paths[(u, v)][1]:
                        ce[(u, v)] = 1.0/len(paths[(u, v)][1])
                else:
                    paths[(u, v)] = (t, {u})

        
        val = sum(ce.values())
        take(times, prev, val)

        for k in lines:
            info[k] = lines[k]
        time, lines = l[index][2], {}
    return TimeCollection(times, detailed)

def ego_at(e, ne, l, at, both, detailed):
    # print >> sys.stderr, "Running node : " + str(e)
    u, v, t, index, times = 0, 0, 0, 0, list()

    ce, info = dict(), dict()
    for i in ne:
        info[(e,i)] = -1
        for x in ne-{i}:
            info[(i,x)] = -1
        info[(i,e)] = -1

    index = 0
    time = l[index][2] #starting time.
    ne_x, lines, paths = ne | {e}, dict(), dict()

    if both:
        def add_lines(u, v, t):
            lines[(u,v)] = t
            lines[(v,u)] = t 
    else:
        def add_lines(u, v, t):
            lines[(u,v)] = t

    prev = None
    while(index < len(l) -1):
        # get all links of time stamp
        while(index < len(l) -1):
            u,v,t = l[index]
            add_lines(u, v, t)
            index += 1
            if(t != time):
                break

        for u in ne:
            for v in ne-{u}:
                # print u,v
                ce[(u,v)] = 0.0
                Q = set()
                if (u,v) not in lines:
                    # print u,v
                    news = info[(u,v)]
                    for x in ne_x - {u, v}:
                        ux = info[(u, x)]
                        if (x, v) in lines:
                            xv = lines[(x, v)]
                        else:
                            xv = info[(x, v)]
                        if ux != -1 and xv != -1 and ux < xv: 
                            if ux == news:
                                Q.add(x)
                            if ux > news:
                                Q = {x}
                                news = ux

                    if (u,v) in paths:
                        old_paths = paths[(u,v)]
                        if old_paths[0] == news:
                            paths[(u, v)] = (news, paths[(u,v)][1] | Q)
                        elif old_paths[0] < news:
                            paths[(u, v)] = (news, Q)
                    else:
                        paths[(u, v)] = (news, Q)

                    if e in paths[(u, v)][1]:
                        ce[(u, v)] = 1.0/len(paths[(u, v)][1])
                else:
                    paths[(u, v)] = (t, {u})

        val = sum(ce.values())
        if prev is None and at < time:
            return .0
        elif at == time:
            return val
        elif prev is not None and at > prev[0] and at < time:
            return prev[1]
        prev = (time, val)

        for k in lines:
            info[k] = lines[k]
        time, lines = l[index][2], {}
    return (prev[1] if prev is not None else .0)
