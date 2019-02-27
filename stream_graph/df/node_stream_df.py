from numbers import Real

import pandas as pd

import stream_graph as sg
from . import utils
from stream_graph import ABC
from .time_set_df import TimeSetDF
from .time_df import IntervalDF
from .time_df import InstantaneousDF
from stream_graph.set import NodeSetS
from stream_graph.set import ITimeSetS
from stream_graph.exceptions import UnrecognizedNodeStream


class NodeStreamDF(ABC.NodeStream):
    def __init__(self, df=None, disjoint_intervals=True, sort_by=['u', 'ts', 'tf']):
        if df is not None:
            if not isinstance(df, IntervalDF):
                if not isinstance(df, pd.DataFrame):
                    df = IntervalDF(list(iter(df)), columns=['u', 'ts', 'tf'])
                else:
                    df = IntervalDF(df)
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
            self.df_.sort_values(by=self.sort_by, inplace=True)
        return self

    @property
    def merge_df_(self):
        if not self.merged_:
            self.df_.union(inplace=True)
            self.merged_ = True
        return self

    @property
    def df(self):
        if bool(self):
            return self.sort_df_.merge_df_.df_
        else:
            return IntervalDF(columns=['u', 'ts', 'tf'])

    @property
    def dfm(self):
        if bool(self):
            return self.merge_df_.df
        else:
            return IntervalDF(columns=['u', 'ts', 'tf'])

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
            return self.df.interval_intersection_size()
        else:
            return 0

    @property
    def size(self):
        if bool(self):
            return self.dfm.measure_time()
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
            return df.index_at_interval(u[1][0], u[1][1]).any()
        else:
            return df.index_at(u[1]).any()

    def __and__(self, ns):
        if isinstance(ns, ABC.NodeStream):
            if ns and bool(self):
                if isinstance(ns, sg.NodeStreamB):
                    return NodeStreamDF(self.df[self.df.u.isin(ns.nodeset)].intersect(utils.ts_to_df(ns.timeset_), by_key=False))
                else:
                    if not isinstance(ns, NodeStreamDF):
                        try:
                            return ns & self
                        except NotImplementedError:
                            pass
                    df = utils.ns_to_df(ns).intersect(self.df)
                if not df.empty:
                    if isinstance(ns, ABC.INodeStream):
                        return INodeStreamDF(df.drop(columns=['tf']))
                    else:
                        return NodeStreamDF(df)
        else:
            raise UnrecognizedNodeStream('second operand')
        return NodeStreamDF()

    def __or__(self, ns):
        if isinstance(ns, ABC.NodeStream):
            if not bool(self):
                return ns.copy()
            if ns:
                if isinstance(ns, sg.NodeStreamB):
                    nst, tdf = ns.nodeset, utils.ts_to_df(ns.timeset_)
                    df = self.df[~self.df.u.isin(nst)].append(
                            self.df[self.df.u.isin(nst)].union(tdf, by_key=False), ignore_index=True)
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
                return NodeStreamDF(self.df.union(utils.ns_to_df(ns)))
            else:
                return self.copy()
        else:
            raise UnrecognizedNodeStream('second operand')
        return NodeStreamDF()

    def __sub__(self, ns):
        if isinstance(ns, ABC.NodeStream):
            if bool(self):
                if ns:
                    if isinstance(ns, sg.NodeStreamB):
                        df = self.df[~self.df.u.isin(ns.nodeset)].append(
                            self.df[self.df.u.isin(ns.nodeset)].difference(
                                utils.ts_to_df(ns.timeset_), by_key=False),
                            ignore_index=True, sort=False)
                    else:
                        df = self.df.difference(utils.ns_to_df(ns))
                    return NodeStreamDF(df)
                else:
                    return self.copy()
        else:
            raise UnrecognizedNodeStream('second operand')
        return NodeStreamDF()

    def node_duration(self, u=None):
        if bool(self):
            return self.df[self.df.u == u].measure_time()
        else:
            return 0

    def node_durations(self):
        node_durations = Counter()
        for u, ts, tf in iter(self):
            node_durations[u] += tf - ts
        return node_durations

    def common_time(self, u=None, v=None):
        if u is None and v is None:
            if not bool(self):
                return dict()
            dfp = df[['u', 'ts']].rename(columns={"ts": "t"})
            dfp['f'] = True
            dfpv = df[['u', 'tf']].rename(columns={"ts": "t"})
            dfpv['f'] = False
            df = dfp.append(dfpv, ignore_index=True).sort_values(by=['t', 'f'])
            active_nodes, common_times = set(), Counter()
            e = df.t.min()
            for u, t, f in df.itertuples(index=False, name=None):
                if f:
                    # start
                    ct = (len(active_nodes) - 1)*(t - e)
                    if ct > .0:
                        for v in active_nodes:
                            common_times[v] += ct
                    active_nodes.add(u)
                else:
                    # finish
                    active_nodes.remove(u)
                    ct = len(active_nodes)*(t - e)
                    if ct > .0:
                        common_times[u] += ct
                        for v in active_nodes:
                            common_times[v] += ct

            return common_times
        else:
            if u is None:
                u, v = v, u
            if bool(self):
                if v is None:
                    idx = (self.df.u == u)
                    if idx.any():
                        a, b = self.df[idx], self.df[~idx]
                        return a.interval_intersection_size(b)
                else:
                    idxa, idxb = (self.df.u == u), (self.df.u == v)
                    if idxa.any() and idxb.any():
                        return self.df[idxa].interval_intersection_size(self.df[idxb])
            return 0.

    # Return what if t not specified?

    def n_at(self, t):
        if bool(self):
            return self.df.df_count_at(t)
        else:
            return 0

    # Return what if t not specified?

    def nodes_at(self, t):
        if bool(self):
            if isinstance(t, tuple) and len(t) is 2 and isinstance(t[0], Real) and isinstance(t[1], Real) and t[0]<=t[1]:
                return NodeSetS(self.df.df_at_interval(t[0], t[1]).u.values.flat)
            elif isinstance(t, Real):
                return NodeSetS(self.df.df_at(t).u.values.flat)
            else:
                raise ValueError('Input can either be a real number or an ascending interval of two real numbers')
        else:
            return NodeSetS()

    def times_of(self, u=None):
        if u is None:
            if bool(self):
                times = defaultdict(list)
                for u, ts, tf in iter(self):
                    times[u].append((ts, tf))
                return {u: TimeSetDF(times) for u, times in iteritems(times)}
            else:
                return dict()
        else:
            if bool(self):
                return TimeSetDF(self.df[self.df.u == u].drop(columns=['u']))
            else:
                return TimeSetDF()

    def issuperset(self, ns):
        if isinstance(ns, ABC.NodeStream):
            if not bool(self):
                return False
            elif bool(ns):
                if isinstance(ns, sg.NodeStreamB):
                    nst = ns.nodeset
                    if nst.issuperset(self.nodeset):
                        return self.df[self.df.u.isin(nst)].issuper(utils.ts_to_df(ns.timeset_), by_key=False)
                else:
                    return not ns or self.df.issuper(utils.ns_to_df(ns))
        else:
            raise UnrecognizedNodeStream('ns')
        return False

class INodeStreamDF(ABC.INodeStream, NodeStreamDF):
    def __init__(self, df=None, disjoint_intervals=True, sort_by=['u', 'ts']):
        if df is not None:
            if not isinstance(df, InstantaneousDF):
                if not isinstance(df, pd.DataFrame):
                    df = InstantaneousDF(list(iter(df)), columns=['u', 'ts'])
                else:
                    df = InstantaneousDF(df)
            self.df_ = df
            self.sort_by = sort_by
            self.merged_ = disjoint_intervals
            self.sorted_ = False

    @property
    def df(self):
        if bool(self):
            return self.sort_df_.merge_df_.df_
        else:
            return InstantaneousDF(columns=['u', 'ts'])

    @property
    def dfm(self):
        if bool(self):
            return self.merge_df_.df
        else:
            return InstantaneousDF(columns=['u', 'ts'])

    @property
    def timeset(self):
        if not bool(self):
            return ITimeSetS()
        return ITimeSetS(self.df_[['ts']].values.flat)

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

        return df.index_at(u[1]).any()

    def __and__(self, ns):
        if isinstance(ns, ABC.NodeStream):
            if isinstance(ns, ABC.INodeStream):
                if ns and bool(self):
                    if not isinstance(ns, INodeStreamDF):
                        try:
                            return ns & self
                        except NotImplementedError:
                            pass
                    df = utils.ins_to_idf(ns).intersect(self.df)
                    if not df.empty:
                        return INodeStreamDF(df)
            else:
                return ns & self
        else:
            raise UnrecognizedNodeStream('second operand')
        return INodeStreamDF()

    def __or__(self, ns):
        if isinstance(ns, ABC.NodeStream):
            if not bool(self):
                return ns.copy()
            if bool(ns):
                if isinstance(ns, ABC.INodeStream):
                    if not isinstance(ns, INodeStreamDF):
                        try:
                            return ns | self
                        except NotImplementedError:
                            pass
                    return INodeStreamDF(self.df.union(utils.ins_to_idf(ns)))
                else:
                    return ns | self
            else:
                return self.copy()
        else:
            raise UnrecognizedNodeStream('second operand')
        return INodeStreamDF()

    def __sub__(self, ns):
        if isinstance(ns, ABC.NodeStream):
            if bool(self):
                if ns:
                    if isinstance(ns, ABC.INodeStream):
                        return INodeStreamDF(self.df.difference(utils.ins_to_idf(ns)))
                    else:
                        df = self.df
                        df['tf'] = df['ts']
                        return INodeStreamDF((NodeStreamDF(df) - ns).df.drop(columns=['tf']))
                else:
                    return self.copy()
        else:
            raise UnrecognizedNodeStream('second operand')
        return INodeStreamDF()

    def nodes_at(self, t):
        if bool(self):
            if isinstance(t, Real):
                return NodeSetS(self.df.df_at(t).u.values.flat)
            else:
                raise ValueError('Input can either be a real number or an ascending interval of two real numbers')
        else:
            return NodeSetS()

    def times_of(self, u=None):
        if bool(self):
            if u is None:
                times = defaultdict(set)
                for u, ts in iter(self):
                    times[u].add(ts)
                return {u: ITimeSetS(s) for u, s in iteritems(times)}
            else:
                return ITimeSetS(self.df[self.df.u == u]['ts'].values.flat)
        else:
            if u is None:
                return ITimeSetS()
            else:
                return dict()

    def issuperset(self, ns):
        if isinstance(ns, ABC.NodeStream):
            if not bool(self):
                return False
            elif bool(ns):
                if isinstance(ns, ABC.INodeStream):
                    return self.df.issuper(utils.ins_to_idf(ns))
                else:
                    df = self.df
                    df['tf'] = df['ts']
                    return NodeStreamDF(df).issuper(ns)
            else:
                return True
        else:
            raise UnrecognizedNodeStream('ns')
        return False

