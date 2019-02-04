"""A file containing utilities for dataframes representing intervals."""
import pandas as pd
import numpy as np

from .time_df import IntervalDF
from .time_df import InstantaneousDF
from stream_graph import ABC

def ns_to_df(ns):
    from .node_stream_df import NodeStreamDF
    from .node_stream_df import INodeStreamDF
    if ns:
        if isinstance(ns, ABC.INodeStream):
            if isinstance(ns, INodeStreamDF):
                df = ns.df
                df['tf'] = df['ts']
                return IntervalDF(df, columns=["u", "ts", "tf"])
            else:
                return IntervalDF(list((u, ts, ts) for u, ts in ns), columns=["u", "ts", "tf"])
        else:
            if isinstance(ns, NodeStreamDF):
                return ns.df
            else:
                return IntervalDF(list(ns), columns=["u", "ts", "tf"])
    else:
        return IntervalDF(columns=["u", "ts", "tf"])

def ins_to_idf(ns):
    from .node_stream_df import INodeStreamDF
    assert isinstance(ns, ABC.INodeStream)
    if ns:
        if isinstance(ns, INodeStreamDF):
            return ns.df
        else:
            return InstantaneousDF(list(ns), columns=["u", "ts"])
    else:
        return InstantaneousDF(columns=["u", "ts"])

def ts_to_df(ts):
    from .time_set_df import TimeSetDF
    if bool(ts):
        if isinstance(ts, ABC.ITimeSet):
            return IntervalDF(list((t, t) for t in ts), columns=["ts", "tf"])
        else:
            if isinstance(ts, TimeSetDF):
                return ts.df
            else:
                return IntervalDF(list(ts), columns=["ts", "tf"])
    else:
        return IntervalDF(columns=["ts", "tf"])

def its_to_idf(ts):
    assert isinstance(ts, ABC.ITimeSet)
    if ts:
        return InstantaneousDF(list(ts), columns=["ts"])
    else:
        return InstantaneousDF(columns=["ts"])

def nsr_disjoint_union(nodes, min_time, max_time, ba, bb):
    return NodeStreamDF().set_df(IntervalDF(iter((n, mn, mx)
                                                 for n in nodes
                                                 for mn, mx
                                                 in (ba, bb)),
                                             columns=["u", "ts", "tf"]),
                                 min_time=min_time,
                                 max_time=max_time)
                                 
