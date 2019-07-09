"""A file containing utilities for dataframes representing intervals."""
from __future__ import absolute_import
from six import string_types

import pandas as pd
import numpy as np

from .dataframes import InstantaneousDF
from .multi_df_utils import init_interval_df, load_interval_df
from stream_graph import ABC
from datetime import timedelta
from collections import Iterable


def tns_to_df(ns):
    from .temporal_node_set_df import TemporalNodeSetDF
    from .itemporal_node_set_df import ITemporalNodeSetDF

    if ns:
        if isinstance(ns, ABC.ITemporalNodeSet):
            if isinstance(ns, ITemporalNodeSetDF):
                return init_interval_df(data=ns.df, keys=['u'], discrete=ns.discrete)
            else:
                return load_interval_df(iter(ns), default_closed=None, disjoint_intervals=(not ns.discrete), keys=["u"], discrete=ns.discrete)
        else:
            if isinstance(ns, TemporalNodeSetDF):
                return ns.df
            else:
                return load_interval_df(iter(ns), disjoint_intervals=True, default_closed=None, keys=["u"])
    else:
        return init_interval_df(keys=['u'], discrete=ns.discrete)


def ins_to_idf(ns):
    from .itemporal_node_set_df import ITemporalNodeSetDF
    assert isinstance(ns, ABC.ITemporalNodeSet)
    if ns:
        if isinstance(ns, ITemporalNodeSetDF):
            return ns.df
        else:
            return InstantaneousDF(list(ns), columns=["u", "ts"])
    else:
        return InstantaneousDF(columns=["u", "ts"])


def ts_to_df(ts):
    from .time_set_df import TimeSetDF

    if bool(ts):
        if isinstance(ts, ABC.ITimeSet):
            return load_interval_df(iter(ts), default_closed=None, disjoint_intervals=(not ts.discrete), keys=["u"], discrete=ts.discrete)
        else:
            if isinstance(ts, TimeSetDF):
                return ts.df
            else:
                return load_interval_df(iter(ts), disjoint_intervals=True, default_closed=None, keys=["u"])
    else:
        return init_interval_df(discrete=ts.discrete)


def its_to_idf(ts):
    assert isinstance(ts, ABC.ITimeSet)
    if ts:
        return InstantaneousDF(list(ts), columns=["ts"])
    else:
        return InstantaneousDF(columns=["ts"])


def t_in(ts, t, L, R):
    # Assumes an sorted-disjoint dataframe
    while L <= R:
        m = int((L + R) / 2)
        if ts[m][0] <= t and ts[m][1] >= t:
            return True
        elif ts[m][0] < t:
            L = m + 1
        elif ts[m][1] > t:
            R = m - 1
    return False


def nsr_disjoint_union(nodes, min_time, max_time, ba, bb):
    return TemporalNodeSetDF().set_df(IntervalDF(iter((n, mn, mx)
                                                 for n in nodes
                                                 for mn, mx
                                                 in (ba, bb)),
                                             columns=["u", "ts", "tf"]),
                                 min_time=min_time,
                                 max_time=max_time)


def make_discrete_bins(bins, bin_size, time_min, time_max):
    if bins is None:
        assert isinstance(bin_size, (int, timedelta))
        if isinstance(bin_size, timedelta):
            bin_size = bin_size.total_seconds()

        # Make bins
        bins = np.arange(time_min, time_max, step).tolist()

        if time_max != bins[-1]:
            bins.append(time_max)
    else:
        assert isinstance(bins, Iterable) and not isinstance(bins, string_types)
        bins = list(bins)

    if len(bins) <= 1:
        raise ValueError('please provide a bigger bin size')
    return bins


def time_discretizer_df(df, bins, bin_size, columns=['ts'], write_protected=True):
    assert isinstance(df, pd.DataFrame)

    if write_protected:
        df = df.copy()

    if isinstance(columns, tuple(set(type(c) for c in df.columns))):
        columns = [columns]

    time_min = min(df[c].min() for c in columns)
    time_max = max(df[c].max() for c in columns)

    bins = make_discrete_bins(bins, bin_size, time_min, time_max)

    assert not len(set(columns) - set(df.columns))

    for c in columns:
        df[c] = pd.cut(df[c], bins, labels=list(range(len(bins) - 1)), include_lowest=True)
    return df.groupby(df.columns.tolist()).size().reset_index().rename(columns={0:'w'}), bins
