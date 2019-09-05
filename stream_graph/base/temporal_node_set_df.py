from __future__ import absolute_import
from numbers import Real
from collections import defaultdict
from collections import Counter
from six import iteritems
from itertools import combinations
from itertools import permutations

import stream_graph as sg
from .utils import ts_to_df, tns_to_df
from .time_set_df import TimeSetDF
from .node_set_s import NodeSetS
from .multi_df_utils import load_interval_df, itertuples_pretty, init_interval_df, build_time_generator, itertuples_raw
from .multi_df_utils import len_set_nodes, set_nodes
from .utils import time_discretizer_df
from stream_graph import ABC
from stream_graph.exceptions import UnrecognizedTemporalNodeSet
from stream_graph.collections import TimeCollection
from stream_graph.collections import TimeGenerator
from stream_graph.collections import NodeCollection
from stream_graph.collections import LinkCollection


class TemporalNodeSetDF(ABC.TemporalNodeSet):
    """DataFrame implementation of ABC.TemporalNodeSet

    Parameters
    ----------
    df: pandas.DataFrame or Iterable, default=None
        If a DataFrame it should contain four columns for u and ts, tf.
        If an Iterable it should produce :code:`(u, ts, tf)` tuples of one NodeId (int or str) and two timestamps (Real) with :code:`ts < tf`.

    disjoint_intervals: Bool, default=False
        Defines if for each node all intervals are disjoint.

    sort_by: Any non-empty subset of ['u', 'ts', 'tf'].
        The order of the DataFrame elements by which they will be produced when iterated.

    """
    def __init__(self, df=None, disjoint_intervals=True, sort_by=None, discrete=None, default_closed='both'):
        if df is not None:
            if isinstance(df, (TemporalNodeSetDF)):
                if bool(df):
                    self.df_ = df.df
                    self.discrete_ = df.discrete
                    self.sort_by = df.sort_by
            else:
                if isinstance(df, ABC.TemporalNodeSet):
                    discrete = df.discrete
                    disjoint_intervals = True
                    df = iter(df)
                self.sort_by = sort_by
                self.df_, self.discrete_ = load_interval_df(df, disjoint_intervals=disjoint_intervals, default_closed=default_closed, discrete=discrete, keys=['u'])
        else:
            self.discrete_ = (True if discrete is None else discrete)

    @property
    def discrete(self):
        return self.discrete_

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
    def is_sorted_(self):
        return (hasattr(self, 'sort_by_') and hasattr(self, 'sorted_') and self.sorted_) or self.sort_by_ is None

    def sort_df(self, sort_by):
        """Retrieve, store if no-order and produce a sorted version of the df"""
        if sort_by is None:
            return self.df
        elif self.sort_by is None:
            self.sort_by = sort_by
            return self.sort_df(sort_by)
        elif self.sort_by == sort_by:
            return self.sorted_df
        else:
            return self.df_.sort_values(by=self.sort_by)

    @property
    def sort_df_(self):
        if not self.is_sorted_:
            self.df_.sort_values(by=self.sort_by, inplace=True)
            self.sorted_ = True
        return self

    @property
    def sorted_df(self):
        if bool(self):
            return self.sort_df_.df_
        else:
            return self._empty_base_class()

    def _empty_base_class(self):
        return init_interval_df(self.discrete_, keys=['u'])

    @property
    def df(self):
        if bool(self):
            return self.df_
        else:
            return self._empty_base_class()

    @property
    def n(self):
        if bool(self):
            return self.df_.u.nunique()
        else:
            return 0

    @property
    def timeset(self):
        if not bool(self):
            return TimeSetDF(discrete=self.discrete)
        return TimeSetDF(self.df.drop(columns=['u']), disjoint_intervals=False, discrete=self.discrete)

    @property
    def nodeset(self):
        if not bool(self):
            return NodeSetS()
        return NodeSetS(self.df.u.drop_duplicates().values.flat)

    @property
    def total_common_time(self):
        # sum of cartesian interval intersection
        if bool(self):
            return self.df.intersection_size(self.df)
        else:
            return 0

    @property
    def size(self):
        if bool(self):
            return self.df.measure_time()
        else:
            return 0

    def __iter__(self):
        if bool(self):
            return itertuples_pretty(self.df, self.discrete)
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

        if isinstance(u[1], tuple) and len(u[1]) in [2, 3]:
            assert len(u[1]) == 2 or u[1][2] in ['both', 'neither', 'left', 'right']
            return df.index_at_interval(*u[1]).any()
        else:
            return df.index_at(u[1]).any()

    def __and__(self, tns):
        if isinstance(tns, ABC.TemporalNodeSet):
            assert self.discrete == tns.discrete
            if tns and bool(self):
                if isinstance(tns, sg.TemporalNodeSetB):
                    return TemporalNodeSetDF(self.df[self.df.u.isin(tns.nodeset)].intersection(ts_to_df(tns.timeset_), by_key=False))
                else:
                    if not isinstance(tns, TemporalNodeSetDF):
                        try:
                            return tns & self
                        except NotImplementedError:
                            pass
                    df = tns_to_df(tns).intersection(self.df)
                if not df.empty:
                    if isinstance(tns, ABC.ITemporalNodeSet):
                        from .itemporal_node_set_df import ITemporalNodeSetDF
                        return ITemporalNodeSetDF(df.drop(columns=['tf'] + ([] if self.discrete else ['s','f'])), discrete=self.discrete)
                    else:
                        return TemporalNodeSetDF(df, discrete=self.discrete)
        else:
            raise UnrecognizedTemporalNodeSet('second operand')
        return TemporalNodeSetDF(discrete=self.discrete)

    def __or__(self, tns):
        if isinstance(tns, ABC.TemporalNodeSet):
            assert tns.discrete == self.discrete
            if not bool(self):
                return tns.copy()
            if tns:
                if isinstance(tns, sg.TemporalNodeSetB):
                    ns, tdf = tns.nodeset, ts_to_df(tns.timeset_)
                    df = self.df[~self.df.u.isin(ns)].append(self.df[self.df.u.isin(ns)].union(tdf, by_key=False), ignore_index=True, merge=False)
                    nstd = ns - self.nodeset
                    if bool(nstd):
                        plist = [(n, ) + key for n in nstd for key in itertuples_raw(tdf, tns.discrete)]
                        dfp = init_interval_df(data=plist, discrete=tns.discrete, keys=['u'], disjoint_intervals=True)
                        df = df.append(dfp, merge=False, ignore_index=True, sort=False)
                    return TemporalNodeSetDF(df.merge(inplace=False), discrete=self.discrete)
                elif not isinstance(tns, TemporalNodeSetDF):
                    try:
                        return tns | self
                    except NotImplementedError:
                        pass
                return TemporalNodeSetDF(self.df.union(tns_to_df(tns)))
            else:
                return self.copy()
        else:
            raise UnrecognizedTemporalNodeSet('second operand')
        return TemporalNodeSetDF(discrete=self.discrete)

    def __sub__(self, tns):
        if isinstance(tns, ABC.TemporalNodeSet):
            assert tns.discrete == self.discrete
            if bool(self):
                if bool(tns):
                    if isinstance(tns, sg.TemporalNodeSetB):
                        dfp = self.df[self.df.u.isin(tns.nodeset)].difference(ts_to_df(tns.timeset_), by_key=False)
                        df = self.df[~self.df.u.isin(tns.nodeset)].append(dfp, ignore_index=True, sort=False)
                    else:
                        df = self.df.difference(tns_to_df(tns))
                    return TemporalNodeSetDF(df, discrete=self.discrete)
                else:
                    return self.copy()
        else:
            raise UnrecognizedTemporalNodeSet('second operand')
        return TemporalNodeSetDF(discrete=self.discrete)

    def duration_of(self, u=None):
        if u is None:
            obj = defaultdict(float)
            dc = (1 if self.discrete else 0)
            for u, ts, tf in self.df.itertuples():
                obj[u] += tf - ts + dc
            return NodeCollection(obj)
        else:
            if bool(self):
                return self.df[self.df.u == u].measure_time()
            else:
                return 0

    def common_time(self, u=None):
        if u is None or self._common_time__list_input(u):
            if not bool(self):
                return NodeCollection(dict())
            df = self.df.events
            active_nodes, common_times = set(), Counter()
            e = df.t.iloc[0]
            if u is None:
                def add_item(active_nodes, ct):
                    for v in (active_nodes):
                        common_times[v] += ct
            else:
                allowed_nodes = set(u)
                def add_item(active_nodes, ct):
                    for v in (active_nodes.intersection(allowed_nodes)):
                        common_times[v] += ct

            dc = (1 if self.discrete else 0)
            for u, t, f in df.itertuples(index=False, name=None):
                ct = (len(active_nodes) - 1)*(t - e + dc)
                if ct > .0:
                    add_item(active_nodes, ct)
                if f:
                    # start
                    active_nodes.add(u)
                else:
                    # finish
                    active_nodes.remove(u)
                e = t

            return NodeCollection(common_times)
        else:
            if bool(self):
                idx = (self.df.u == u)
                if idx.any():
                    a, b = self.df[idx], self.df[~idx]
                    return a.intersection_size(b)
            return 0.

    def common_time_pair(self, l=None):
        if l is None or self._common_time_pair__list_input(l):
            if not bool(self):
                return LinkCollection(dict())
            df = self.df.events
            active_nodes = set()
            e = df.t.iloc[0]
            if l is None:
                common_times = Counter()
                def add_item(active_nodes, ct):
                    for u, v in combinations(active_nodes, 2):
                        common_times[(u, v)] += ct
            else:
                links = set(l)
                common_times = {l: 0 for l in links}
                allowed_nodes = set(c for a, b in links for c in [a, b])
                def add_item(active_nodes, ct):
                    active_set = active_nodes & allowed_nodes
                    if len(common_times) <= len(active_set)*(len(active_set) - 1)/2:
                        for (u, v) in common_times.keys():
                            if u in active_set and v in active_set:
                                common_times[(u, v)] += ct
                    else:
                        for u, v in combinations(active_set, 2):
                            if (u, v) in common_times:
                                common_times[(u, v)] += ct
                            if (v, u) in common_times:
                                common_times[(v, u)] += ct

            dc = (1 if self.discrete else 0)
            for u, t, f in df.itertuples(index=False, name=None):
                ct = (len(active_nodes) - 1)*(t - e + dc)
                if ct > .0:
                    add_item(active_nodes, ct)
                if f:
                    # start
                    active_nodes.add(u)
                else:
                    # finish
                    active_nodes.remove(u)
                e = t
            return LinkCollection(common_times)
        else:
            u, v = l
            if bool(self):
                idxa, idxb = (self.df.u == u), (self.df.u == v)
                if idxa.any() and idxb.any():
                    return self.df[idxa].intersection_size(self.df[idxb])
            return 0.

    def _build_time_generator(self, cache_constructor, calculate, tc, df=None, **kargs):
        if df is None:
            df = self.df
        return tc(build_time_generator(df, cache_constructor=cache_constructor, calculate=calculate, **kargs), discrete=self.discrete, instantaneous=False)

    def n_at(self, t=None):
        if bool(self):
            if t is None:
                return self._build_time_generator(set, len_set_nodes, TimeCollection)
            else:
                return self.df.count_at(t)
        else:
            if t is None:
                return TimeCollection([], False)
            else:
                return 0

    def nodes_at(self, t=None):
        if bool(self):
            if t is None:
                return self._build_time_generator(set, set_nodes, TimeGenerator)

            elif isinstance(t, tuple) and len(t) in [2, 3] and isinstance(t[0], Real) and isinstance(t[1], Real) and t[0]<=t[1]:
                assert len(t) == 2 or t[2] in ['neither', 'both', 'left', 'right']
                return NodeSetS(self.df.df_at_interval(*t).u.values.flat)
            elif isinstance(t, Real):
                return NodeSetS(self.df.df_at(t).u.values.flat)
            else:
                raise ValueError('Input can either be a real number or an ascending interval of two real numbers')
        else:
            if t is None:
                return TimeGenerator(iter())
            else:
                return NodeSetS()

    def times_of(self, u=None):
        if u is None:
            if bool(self):
                times = defaultdict(list)
                for key in itertuples_raw(self.df, discrete=self.discrete):
                    times[key[0]].append(key[1:])
                return NodeCollection({u: TimeSetDF(init_interval_df(data=ts, discrete=self.discrete), discrete=self.discrete, disjoint_intervals=True) for u, ts in iteritems(times)})
            else:
                return NodeCollection(dict())
        else:
            if bool(self):
                return TimeSetDF(self.df[self.df.u == u].drop(columns=['u'], merge=True))
            else:
                return TimeSetDF()

    def issuperset(self, tns):
        if isinstance(tns, ABC.TemporalNodeSet):
            assert self.discrete == tns.discrete
            if not bool(self):
                return False
            elif bool(tns):
                if isinstance(tns, sg.TemporalNodeSetB):
                    ns = tns.nodeset
                    if ns.issuperset(self.nodeset):
                        return self.df[self.df.u.isin(ns)].issuper(ts_to_df(tns.timeset_), by_key=False)
                else:
                    return not tns or self.df.issuper(tns_to_df(tns))
        else:
            raise UnrecognizedTemporalNodeSet('ns')
        return False

    def substream(self, nsu=None, ts=None):
        if nsu is not None:
            if not isinstance(nsu, ABC.NodeSet):
                try:
                    nsu = NodeSetS(nsu)
                except Exception:
                    raise UnrecognizedNodeSet('nsu')
        if ts is not None:
            if not isinstance(ts, ABC.TimeSet):
                try:
                    ts = TimeSetDF(ts, discrete=self.discrete)
                except Exception:
                    raise UnrecognizedTimeSet('ts')
        if all(o is None for o in [nsu, ts]):
            return self.copy()
        if bool(self) and all((o is None or bool(o)) for o in [nsu, ts]):
            if nsu is not None:
                df = self.df[self.df.u.isin(nsu)]
            else:
                df = self.df

            if ts is not None:
                df = df.intersection(ts_to_df(ts), by_key=False, on_column=['u'])
            return self.__class__(df, discrete=self.discrete, weighted=self.weighted)
        else:
            return self.__class__()

    def _to_discrete(self, bins, bin_size):
        df, bins = time_discretizer_df(self.df, bins, bin_size, columns=['ts', 'tf'])
        return self.__class__(df, disjoint_intervals=False, discrete=True), bins
