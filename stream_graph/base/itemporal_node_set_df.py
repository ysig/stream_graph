from __future__ import absolute_import
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
from .instantaneous_df import InstantaneousDF
from .node_set_s import NodeSetS
from .itime_set_s import ITimeSetS
from stream_graph.exceptions import UnrecognizedTemporalNodeSet
from stream_graph.collections import TimeCollection
from stream_graph.collections import TimeGenerator
from stream_graph.collections import NodeCollection
from stream_graph.collections import LinkCollection

class ITemporalNodeSetDF(ABC.ITemporalNodeSet):
    """DataFrame implementation of ABC.ITemporalNodeSet"""
    def __init__(self, df=None, no_duplicates=True, sort_by=['u', 'ts'], discrete=None):
        """Initialize a Temporal Node Set.

        Parameters
        ----------
        df: pandas.DataFrame or Iterable, default=None
            If a DataFrame it should contain two columns for u and ts.
            If an Iterable it should produce :code:`(u, ts)` tuples of one NodeId (int or str) and a timestamp (Real).

        no_duplicates: Bool, default=True
            Defines if for each node all intervals are disjoint.

        sort_by: Any non-empty subset of ['u', 'ts'], default=['u', 'ts'].
            The order of the DataFrame elements by which they will be produced when iterated.

        """
        if df is not None:
            if not isinstance(df, InstantaneousDF):
                if not isinstance(df, pd.DataFrame):
                    if isinstance(df, ABC.TemporalNodeSet):
                        discrete = df.discrete
                    df = list(iter(df))
                df = InstantaneousDF(df, columns=['u', 'ts'])
            self.df_ = df
            self.sort_by = sort_by
            self.merged_ = no_duplicates
            self.sorted_ = False
            if discrete is None:
                discrete = False
            self.discrete_ = discrete
            if discrete and self.df_['ts'].dtype.kind != 'i':
                warn('SemanticWarning: For a discrete instance time-instants should be an integers')

    @property
    def discrete(self):
        return (self.discrete_ if bool(self) else None)

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
            self.df_.merge(inplace=True)
            self.merged_ = True
        return self

    @property
    def nodeset(self):
        if not bool(self):
            return NodeSetS()
        return NodeSetS(self.df_.u.drop_duplicates().values.flat)

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
        return ITimeSetS(self.df_[['ts']].values.flat, discrete=self.discrete)

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
        if isinstance(ns, ABC.TemporalNodeSet):
            if isinstance(ns, ABC.ITemporalNodeSet):
                if ns and bool(self):
                    assert ns.discrete == self.discrete
                    if not isinstance(ns, self.__class__):
                        try:
                            return ns & self
                        except NotImplementedError:
                            pass
                    df = utils.ins_to_idf(ns).intersect(self.df)
                    if not df.empty:
                        return self.__class__(df, discrete=self.discrete)
            else:
                return ns & self
        else:
            raise UnrecognizedTemporalNodeSet('second operand')
        return self.__class__()

    def __or__(self, ns):
        if isinstance(ns, ABC.TemporalNodeSet):
            if not bool(self):
                return ns.copy()
            if bool(ns):
                if isinstance(ns, ABC.ITemporalNodeSet):
                    if not isinstance(ns, self.__class__):
                        try:
                            return ns | self
                        except NotImplementedError:
                            pass
                    return self.__class__(self.df.union(utils.ins_to_idf(ns)), discrete=self.discrete)
                else:
                    return ns | self
            else:
                return self.copy()
        else:
            raise UnrecognizedTemporalNodeSet('second operand')
        return self.__class__()

    def __sub__(self, ns):
        if isinstance(ns, ABC.TemporalNodeSet):
            if bool(self):
                if ns:
                    assert ns.discrete == self.discrete
                    if isinstance(ns, ABC.ITemporalNodeSet):
                        return self.__class__(self.df.difference(utils.ins_to_idf(ns)), discrete=self.discrete)
                    else:
                        df = self.df
                        df['tf'] = df['ts']
                        return self.__class__((TemporalNodeSetDF(df, discrete=self.discrete) - ns).df.drop(columns=['tf']), discrete=self.discrete)
                else:
                    return self.copy()
        else:
            raise UnrecognizedTemporalNodeSet('second operand')
        return self.__class__()

    def nodes_at(self, t=None):
        if bool(self):
            if t is None:
                def generate(df):
                    for _, df_grouped in df:
                        ts = df_grouped.iloc[0]['ts']
                        yield (ts, NodeSetS(set(df_grouped.u.values)))
                return TimeGenerator(generate(self.dfm.groupby('ts')), True)
            elif isinstance(t, Real):
                return NodeSetS(self.df.df_at(t).u.values.flat)
            else:
                raise ValueError('Input can either be a real number or an ascending interval of two real numbers')
        else:
            if t is None:
                return TimeCollection(iter(), True)
            else:
                return NodeSetS()

    def times_of(self, u=None):
        if bool(self):
            if u is None:
                times = defaultdict(set)
                for u, ts in iter(self):
                    times[u].add(ts)
                return NodeCollection({u: ITimeSetS(s, discrete=self.discrete) for u, s in iteritems(times)})
            else:
                return ITimeSetS(self.df[self.df.u == u]['ts'].values.flat, discrete=self.discrete)
        else:
            if u is None:
                return ITimeSetS()
            else:
                return NodeCollection(dict())

    def issuperset(self, ns):
        if isinstance(ns, ABC.TemporalNodeSet):
            if not bool(self):
                return False
            elif bool(ns):
                assert ns.discrete == self.discrete
                if isinstance(ns, ABC.ITemporalNodeSet):
                    return self.df.issuper(utils.ins_to_idf(ns))
                else:
                    df = self.df
                    df['tf'] = df['ts']
                    return TemporalNodeSetDF(df, discrete=self.discrete).issuper(ns)
            else:
                return True
        else:
            raise UnrecognizedTemporalNodeSet('ns')
        return False

    @property
    def _total_common_time_discrete(self):
        ct = 0
        if bool(self):
            for _, df_grouped in self.df:
                if df_grouped.shape[0] > 1:
                    ct += (df_grouped.shape[0]-1)*df_grouped.shape[0]/2
        return ct

    @property
    def _size_discrete(self):
        if bool(self):
            return self.df.shape[0]
        else:
            return 0

    def _node_duration_discrete(self, u=None):
        if u is None:
            return NodeCollection(Counter(self.df.u.values.flat))
        else:
            if bool(self):
                return (self.df.u == u).sum()
            else:
                return 0

    def _common_time_discrete(self, u=None):
        if u is None or self._common_time__list_input(u):
            ct = defaultdict(int)
            if u is None:
                for _, df_grouped in self.df.groupby('ts'):
                    if df_grouped.shape[0] > 1:
                        for u in df_grouped.u:
                            ct[u] += (df_grouped.shape[0]-1)
            else:
                valid_nodes = set(u)
                for _, df_grouped in self.df.groupby('ts'):
                    if df_grouped.shape[0] > 1:
                        for u in df_grouped.u:
                            if u in valid_nodes:
                                ct[u] += (df_grouped.shape[0]-1)                
            return NodeCollection(ct)
        else:
            if bool(self):
                idx = (self.df.u == u)
                if idx.any():
                    a, b = self.df[idx], self.df[~idx]
                    return a.interval_intersection_size(b, discrete=self.discrete)
            return 0.

    def _common_time_pair_discrete(self, l=None):
        if l is None or self._common_time_pair__list_input(l):
            ct = defaultdict(int)
            if l is None:
                for _, df_grouped in self.df.groupby('ts'):
                    if df_grouped.shape[0] > 1:
                        for l in combinations(df_grouped.u, 2):
                            ct[l] += 1
            else:
                valid_links = set(l)
                valid_nodes = set(u for l in valid_links for u in l)
                for _, df_grouped in self.df.groupby('ts'):
                    if df_grouped.shape[0] > 1:
                        for l in combinations(set(df_grouped.u.values.flat) & valid_nodes, 2):
                            if l in valid_links:
                                ct[u] += 1
            return NodeCollection(ct)
        else:
            u, v = l
            if bool(self):
                idxa, idxb = (self.df.u == u), (self.df.u == v)
                if idxa.any() and idxb.any():
                    return self.df[idxa].interval_intersection_size(self.df[idxb], discrete=self.discrete)
                idxa, idxb = (self.df.u == u), (self.df.u == v)
                if idxa.any() and idxb.any():
                    return self.df[idxa].interval_intersection_size(self.df[idxb], discrete=self.discrete)
            return 0.

    def _to_discrete(self, bins, bin_size):
        df, bins = time_discretizer_df(self.df, bins, bin_size, columns=['ts'])
        return self.__class__(df, no_duplicates=False, discrete=True), bins
