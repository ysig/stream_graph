from numbers import Real

import pandas as pd

import stream_graph as sg
from . import utils
from stream_graph import API
from .time_set_df import TimeSetDF
from stream_graph.set import NodeSetS
from stream_graph.exceptions import UnrecognizedNodeStream


class NodeStreamDF(API.NodeStream):
    def __init__(self, df=None, disjoint_intervals=True, sort_by=['u', 'ts', 'tf']):
        if df is not None:
            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame(list(iter(df)), columns=['u', 'ts', 'tf'])
            self.df_ = df
            self.sort_by = sort_by
            self.merged_ = disjoint_intervals
            self.sorted_ = False

    @property
    def sort_by(self):
        if hasattr(self, 'sort_by_'):
            return self.sort_by_
        else:
            return None

    @sort_by.setter
    def sort_by(self, value):
        if not (hasattr(self, 'sort_by_') and self.sort_by_ == value):
            self.sorted_ = False
            self.sort_by_ = value

    @property
    def is_merged_(self):
        return hasattr(self, 'merged_') and self.merged_

    @property
    def is_sorted_(self):
        return (hasattr(self, 'sort_by_') and hasattr(self, 'sorted_') and self.sorted_) or self.sort_by_ is None

    @property
    def sort_df_(self):
        if not self.is_sorted_:
            self.df_ = self.df_.sort_values(by=self.sort_by)
        return self

    @property
    def merge_df_(self):
        if not self.merged_:
            self.df_ = utils.merge_intervals_df(self.df_)
            self.merged_ = True
        return self

    @property
    def df(self):
        if bool(self):
            return self.sort_df_.merge_df_.df_
        else:
            return pd.DataFrame(columns=['u', 'ts', 'tf'])

    @property
    def dfm(self):
        if bool(self):
            return self.merge_df_.df
        else:
            return pd.DataFrame(columns=['u', 'ts', 'tf'])

    @property
    def n(self):
        if bool(self):
            return self.df_.u.nunique()
        else:
            return 0

    @property
    def timeset(self):
        if not bool(self):
            return TimeSetDF()
        return TimeSetDF(self.df_[['ts', 'tf']], disjoint_intervals=False)

    @property
    def nodeset(self):
        if not bool(self):
            return NodeSetS()
        return NodeSetS(self.df_.u.drop_duplicates().values.flat)

    @property
    def total_common_time(self):
        # sum of cartesian interval intersection
        if bool(self):
            return utils.interval_intersection_size(self.df)
        else:
            return 0

    @property
    def size(self):
        if bool(self):
            return utils.measure_time(self.df)
        else:
            return 0

    def __iter__(self):
        if bool(self):
            return self.df.itertuples(index=False, name=None)
        else:
            return iter([])

    def __bool__(self):
        return hasattr(self, 'df_') and not self.df_.empty

    def __contains__(self, u):
        assert type(u) is tuple and len(u) is 2
        if (not bool(self)) or (u[0] is None and u[1] is None):
            return False

        if u[0] is None:
            df = self.df
        elif u[1] is None:
            return (self.df.u == u[0]).any()
        else:
            df = self.df[self.df.u == u[0]]

        if isinstance(u[1], tuple) and len(u[1]) == 2:
            return utils.df_index_at_interval(df, u[1][0], u[1][1]).any()
        else:
            return utils.df_index_at(df, u[1]).any()

    def __and__(self, ns):
        if isinstance(ns, API.NodeStream):
            if ns and bool(self):
                if isinstance(ns, sg.NodeStreamB):
                    return NodeStreamDF(utils.intersect_intervals_with_df(
                        self.df[self.df.u.isin(ns.nodeset)],
                        utils.ts_to_df(ns.timeset_)))
                else:
                    if not isinstance(ns, NodeStreamDF):
                        try:
                            return ns & self
                        except NotImplementedError:
                            pass
                    df = utils.intersect_intervals_df(
                        utils.ns_to_df(ns).append(self.df,
                                                  ignore_index=True,
                                                  sort=True))
                if not df.empty:
                    return NodeStreamDF(df)
        else:
            raise UnrecognizedNodeStream('second operand')
        return NodeStreamDF()

    def __or__(self, ns):
        if isinstance(ns, API.NodeStream):
            if not bool(self):
                return ns.copy()
            if ns:
                if isinstance(ns, sg.NodeStreamB):
                    nst, tdf = ns.nodeset, utils.ts_to_df(ns.timeset_)
                    df = self.df[~self.df.u.isin(nst)].append(
                            utils.merge_intervals_with_df(
                                self.df[self.df.u.isin(nst)],
                                tdf), ignore_index=True, sort=False)
                    nstd = nst - self.nodeset
                    if bool(nstd):
                        df = df.append(pd.DataFrame(
                            list((n, ts, tf) for n in nstd for ts, tf in tdf),
                            columns=['u', 'ts', 'tf']),
                                ignore_index=True, sort=False)
                    return NodeStreamDF(df)
                elif not isinstance(ns, NodeStreamDF):
                    try:
                        return ns | self
                    except NotImplementedError:
                        pass
                return NodeStreamDF(utils.merge_intervals_df(
                        self.df.append(utils.ns_to_df(ns), ignore_index=True)))
            else:
                return self.copy()
        else:
            raise UnrecognizedNodeStream('second operand')
        return NodeStreamDF()

    def __sub__(self, ns):
        if isinstance(ns, API.NodeStream):
            if bool(self):
                if ns:
                    if isinstance(ns, sg.NodeStreamB):
                        df = self.df[~self.df.u.isin(ns.nodeset)].append(
                            utils.difference_with_df(
                                self.df[self.df.u.isin(ns.nodeset)],
                                utils.ts_to_df(ns.timeset_)),
                                ignore_index=True, sort=False)
                    else:
                        df = utils.difference_df(self.df, utils.ns_to_df(ns))
                    return NodeStreamDF(df)
                else:
                    return self.copy()
        else:
            raise UnrecognizedNodeStream('second operand')
        return NodeStreamDF()

    def node_duration(self, u):
        if bool(self):
            return utils.measure_time(self.df[self.df.u == u])
        else:
            return 0

    def common_time(self, u, v=None):
        if bool(self):
            if v is None:
                idx = (self.df.u == u)
                if idx.any():
                    a, b = self.df[idx], self.df[~idx]
                    return utils.interval_intersection_size(a, b)
            else:
                idxa, idxb = (self.df.u == u), (self.df.u == v)
                if idxa.any() and idxb.any():
                    return utils.interval_intersection_size(self.df[idxa], self.df[idxb])
        return 0.

    def number_of_nodes_at(self,  t):
        if bool(self):
            return utils.df_count_at(self.df, t)
        else:
            return 0

    def nodes_at(self, t):
        if bool(self):
            if isinstance(t, tuple) and len(t) is 2 and isinstance(t[0], Real) and isinstance(t[1], Real) and t[0]<=t[1]:
                return NodeSetS(utils.df_at_interval(self.df, t[0], t[1]).u.values.flat)
            elif isinstance(t, Real):
                return NodeSetS(utils.df_at(self.df, t).u.values.flat)
            else:
                raise ValueError('Input can either be a real number or an ascending interval of two real numbers')
        else:
            return 0

    def times_of(self, u):
        if bool(self):
            return TimeSetDF(self.df[self.df.u == u].drop(columns=['u']), disjoint_intervals=False)
        else:
            return 0

    def issuperset(self, ns):
        if isinstance(ns, API.NodeStream):
            if not bool(self):
                return False
            elif bool(ns):
                if isinstance(ns, sg.NodeStreamB):
                    nst = ns.nodeset
                    if nst.issuperset(self.nodeset):
                        return utils.issuper_with_df(self.df[self.df.u.isin(nst)],
                                                     utils.ts_to_df(ns.timeset_))
                else:
                    return not ns or utils.issuper_df(self.df, utils.ns_to_df(ns))
        else:
            raise UnrecognizedNodeStream('ns')
        return False
