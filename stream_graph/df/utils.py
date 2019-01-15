"""A file containing utilities for dataframes representing intervals."""
import pandas as pd
import numpy as np

from .interval_df import IntervalDF

def ns_to_df(ns):
    from .node_stream_df import NodeStreamDF
    if ns:
        if isinstance(ns, NodeStreamDF):
            return ns.df
        else:
            return IntervalDF(list(ns), columns=["u", "ts", "tf"])
    else:
        return IntervalDF(columns=["u", "ts", "tf"])

def ts_to_df(ts):
    from .time_set_df import TimeSetDF
    if bool(ts):
        if isinstance(ts, TimeSetDF):
            return ts.df
        else:
            return IntervalDF(list(ts), columns=["u", "ts", "tf"])
    else:
        return IntervalDF(columns=["u", "ts", "tf"])

def nsr_disjoint_union(nodes, min_time, max_time, ba, bb):
    return NodeStreamDF().set_df(IntervalDF(iter((n, mn, mx)
                                                 for n in nodes
                                                 for mn, mx
                                                 in (ba, bb)),
                                             columns=["u", "ts", "tf"]),
                                 min_time=min_time,
                                 max_time=max_time)
