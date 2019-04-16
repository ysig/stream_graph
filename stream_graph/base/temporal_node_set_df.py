from numbers import Real
from warnings import warn
from collections import defaultdict
from collections import Iterable
from collections import Counter
from six import iteritems
from itertools import combinations
from itertools import permutations

import pandas as pd

import stream_graph as sg
from . import utils
from stream_graph import ABC
from .time_set_df import TimeSetDF
from .interval_df import IntervalDF
from .node_set_s import NodeSetS
from .itime_set_s import ITimeSetS
from stream_graph.exceptions import UnrecognizedTemporalNodeSet
from stream_graph.collections import TimeCollection
from stream_graph.collections import TimeGenerator
from stream_graph.collections import NodeCollection
from stream_graph.collections import LinkCollection

class TemporalNodeSetDF(ABC.TemporalNodeSet):
    """DataFrame implementation of ABC.TemporalNodeSet"""
    def __init__(self, df=None, disjoint_intervals=True, sort_by=['u', 'ts', 'tf'], discrete=None):
        """Initialize a Temporal Node Set.

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
        if df is not None:
            if isinstance(df, ABC.TemporalNodeSet):
                discrete = df.discrete
            elif discrete is None:
                discrete = False
            kargs = {}
            if not isinstance(df, (IntervalDF, pd.DataFrame)):
                df = IntervalDF(list(iter(df)), columns=['u', 'ts', 'tf'], **kargs)
            else:
                df = IntervalDF(df, **kargs)
            self.df_ = df
            self.sort_by = sort_by
            self.merged_ = disjoint_intervals
            self.sorted_ = False
            self.discrete_ = discrete
            if discrete and self.df_['ts'].dtype.kind != 'i' and self.df_['tf'].dtype.kind != 'i':
                warn('SemanticWarning: For a discrete instance time-instants should be an integers')

    @property
    def discrete(self):
        if bool(self):
            return self.discrete_
        else:
            return None

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
            self.sorted_ = True
        return self

    @property
    def merge_df_(self):
        if not self.merged_:
            self.df_.merge(inplace=True)
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
            return TimeSetDF(discrete=self.discrete)
        return TimeSetDF(self.df_[['ts', 'tf']], disjoint_intervals=False, discrete=self.discrete)

    @property
    def nodeset(self):
        if not bool(self):
            return NodeSetS()
        return NodeSetS(self.df_.u.drop_duplicates().values.flat)

    @property
    def total_common_time(self):
        # sum of cartesian interval intersection
        if bool(self):
            return self.df.interval_intersection_size(discrete=self.discrete)
        else:
            return 0

    @property
    def size(self):
        if bool(self):
            return self.dfm.measure_time(discrete=self.discrete)
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
        if isinstance(ns, ABC.TemporalNodeSet):
            assert ns.discrete == self.discrete
            if ns and bool(self):
                if isinstance(ns, sg.TemporalNodeSetB):
                    return TemporalNodeSetDF(self.df[self.df.u.isin(ns.nodeset)].intersect(utils.ts_to_df(ns.timeset_), by_key=False))
                else:
                    if not isinstance(ns, TemporalNodeSetDF):
                        try:
                            return ns & self
                        except NotImplementedError:
                            pass
                    df = utils.ns_to_df(ns).intersect(self.df)
                if not df.empty:
                    if isinstance(ns, ABC.ITemporalNodeSet):
                        return ITemporalNodeSetDF(df.drop(columns=['tf']), discrete=self.discrete)
                    else:
                        return TemporalNodeSetDF(df, discrete=self.discrete)
        else:
            raise UnrecognizedTemporalNodeSet('second operand')
        return TemporalNodeSetDF(discrete=self.discrete)

    def __or__(self, ns):
        if isinstance(ns, ABC.TemporalNodeSet):
            assert ns.discrete == self.discrete
            if not bool(self):
                return ns.copy()
            if ns:
                if isinstance(ns, sg.TemporalNodeSetB):
                    nst, tdf = ns.nodeset, utils.ts_to_df(ns.timeset_)
                    df = self.df[~self.df.u.isin(nst)].append(
                            self.df[self.df.u.isin(nst)].union(tdf, by_key=False), ignore_index=True)
                    nstd = nst - self.nodeset
                    if bool(nstd):
                        df = df.append(pd.DataFrame(
                            list((n, ts, tf) for n in nstd for ts, tf in tdf),
                            columns=['u', 'ts', 'tf']),
                            ignore_index=True, sort=False)
                    return TemporalNodeSetDF(df, discrete=self.discrete)
                elif not isinstance(ns, TemporalNodeSetDF):
                    try:
                        return ns | self
                    except NotImplementedError:
                        pass
                return TemporalNodeSetDF(self.df.union(utils.ns_to_df(ns)), discrete=self.discrete)
            else:
                return self.copy()
        else:
            raise UnrecognizedTemporalNodeSet('second operand')
        return TemporalNodeSetDF(discrete=self.discrete)

    def __sub__(self, ns):
        if isinstance(ns, ABC.TemporalNodeSet):
            assert ns.discrete == self.discrete
            if bool(self):
                if ns:
                    if isinstance(ns, sg.TemporalNodeSetB):
                        df = self.df[~self.df.u.isin(ns.nodeset)].append(
                            self.df[self.df.u.isin(ns.nodeset)].difference(
                                utils.ts_to_df(ns.timeset_), by_key=False),
                            ignore_index=True, sort=False)
                    else:
                        df = self.df.difference(utils.ns_to_df(ns))
                    return TemporalNodeSetDF(df, discrete=self.discrete)
                else:
                    return self.copy()
        else:
            raise UnrecognizedTemporalNodeSet('second operand')
        return TemporalNodeSetDF(discrete=self.discrete)

    def node_duration(self, u=None):
        if u is None:
            obj = defaultdict(float)
            dc = (1 if self.discrete else 0)
            for u, ts, tf in self.dfm.itertuples(index=False, name=None):
                obj[u] += tf - ts + dc
            return NodeCollection(obj)
        else:
            if bool(self):
                return self.df[self.df.u == u].measure_time(self.discrete)
            else:
                return 0

    def common_time(self, u=None):
        if u is None or self._common_time__list_input(u):
            if not bool(self):
                return NodeCollection(dict())
            df = self.dfm.events
            active_nodes, common_times = set(), Counter()
            e = df.t.min()
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
                    return a.interval_intersection_size(b, discrete=self.discrete)
            return 0.

    def common_time_pair(self, l=None):
        if l is None or self._common_time_pair__list_input(l):
            if not bool(self):
                return LinkCollection(dict())
            df = self.dfm.events
            active_nodes = set()
            e = df.t.min()
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
                    for u, v in permutations(active_nodes & allowed_nodes, 2):
                        if (u, v) in common_times:
                            common_times[(u, v)] += ct

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
                    return self.df[idxa].interval_intersection_size(self.df[idxb], discrete=self.discrete)
            return 0.

    # Return what if t not specified?
    def n_at(self, t=None):
        if bool(self):
            if t is None:
                df = self.dfm.events
                u, e, f = df.iloc[0]
                active_nodes = {u}
                nodes = list()
                for u, t, f in df.iloc[1:].itertuples(index=False, name=None):
                    if f:
                        # start
                        if t > e:
                            nodes.append((e, len(active_nodes)))
                        active_nodes.add(u)
                    else:
                        # finish
                        if t > e:
                            nodes.append((e, len(active_nodes)))
                        active_nodes.remove(u)
                    e = t
                ef, nan = nodes[-1]
                if e != ef or nan != len(active_nodes):
                    nodes.append((e, len(active_nodes)))
                return TimeCollection(nodes, False)
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
                def generate(df):
                    u, e, f = df.iloc[0]
                    active_nodes = {u}
                    for u, t, f in df.iloc[1:].itertuples(index=False, name=None):
                        if t > e:
                            ef, san = e, set(active_nodes)
                            yield (ef, NodeSetS(san))
                        if f:
                            # start                        
                            active_nodes.add(u)
                        else:
                            # finish
                            active_nodes.remove(u)
                        e = t
                    if e != ef or san != active_nodes:
                        yield (e, NodeSetS(set(active_nodes)))

                return TimeGenerator(generate(self.dfm.events))

            elif isinstance(t, tuple) and len(t) is 2 and isinstance(t[0], Real) and isinstance(t[1], Real) and t[0]<=t[1]:
                return NodeSetS(self.df.df_at_interval(t[0], t[1]).u.values.flat)
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
                for u, ts, tf in iter(self):
                    times[u].append((ts, tf))
                return NodeCollection({u: TimeSetDF(times, discrete=self.discrete) for u, times in iteritems(times)})
            else:
                return NodeCollection(dict())
        else:
            if bool(self):
                return TimeSetDF(self.df[self.df.u == u].drop(columns=['u']), discrete=self.discrete)
            else:
                return TimeSetDF()

    def issuperset(self, ns):
        if isinstance(ns, ABC.TemporalNodeSet):
            assert ns.discrete == self.discrete
            if not bool(self):
                return False
            elif bool(ns):
                if isinstance(ns, sg.TemporalNodeSetB):
                    nst = ns.nodeset
                    if nst.issuperset(self.nodeset):
                        return self.df[self.df.u.isin(nst)].issuper(utils.ts_to_df(ns.timeset_), by_key=False)
                else:
                    return not ns or self.df.issuper(utils.ns_to_df(ns))
        else:
            raise UnrecognizedTemporalNodeSet('ns')
        return False

    def _to_discrete(self, bins, bin_size):
        df, bins = time_discretizer_df(self.df, bins, bin_size, columns=['ts', 'tf'])
        return self.__class__(df, disjoint_intervals=False, discrete=True), bins
