from __future__ import absolute_import
import operator
from functools import reduce
from pandas import DataFrame as DF
from numbers import Real
from collections import defaultdict
from collections import Counter
from collections import Iterable
from warnings import warn
from six import iteritems

from .utils import tns_to_df, ts_to_df, t_in, its_to_idf, ins_to_idf, time_discretizer_df, make_algebra
from . import functions
from stream_graph import ABC
from .itemporal_node_set_df import ITemporalNodeSetDF
from .link_set_df import LinkSetDF
from .node_set_s import NodeSetS
from .temporal_node_set_b import TemporalNodeSetB
from .temporal_link_set_df import TemporalLinkSetDF
from .itime_set_s import ITimeSetS
from .time_set_df import TimeSetDF
from .multi_df_utils import init_instantaneous_df, load_instantaneous_df, class_interval_df
from stream_graph.collections import NodeCollection
from stream_graph.collections import LinkCollection
from stream_graph.collections import TimeGenerator
from stream_graph.collections import TimeCollection
from stream_graph.exceptions import UnrecognizedTemporalLinkSet
from stream_graph.exceptions import UnrecognizedTemporalNodeSet
from stream_graph.exceptions import UnrecognizedNodeSet
from stream_graph.exceptions import UnrecognizedTimeSet
from stream_graph.exceptions import UnrecognizedDirection

class ITemporalLinkSetDF(ABC.ITemporalLinkSet):
    """DataFrame implementation of ABC.ITemporalLinkSet.

    Parameters
    ----------
    df: pandas.DataFrame or Iterable, default=None
        If a DataFrame it should contain three columns for u and v and ts.
        If an Iterable it should produce :code:`(u, v, ts)` tuples of two NodeId (int or str) and one timestamps (Real) with :code:`ts`.

    no_duplicates: Bool, default=False
        Defines if for each link there are no duplicate timestamps.

    sort_by: Any non-empty subset of ['u', 'v', 'ts'].
        The order of the DataFrame elements by which they will be produced when iterated.

    discrete : bool, or default=None.

    weighted : bool, or default=None.

    merge_function : A function applied to a list of arguments.

    operation_functions: dict {str: fun}
        A dictionary of names of operations, i.e. :code:`union/u`, :code:`intersection/i`, :code:`difference/d`, :code:`issuperset/s'.
        All function should be applicable between two weights.
        Default: +, min, hinge_loss (ignoring an interval on zero), operator.ge (ignoring an interval on zero)

    """
    def __init__(self, df=None, no_duplicates=True, sort_by=None, discrete=None, weighted=None, merge_function=None, operation_functions=None):

        if isinstance(df, self.__class__):
            self.discrete_ = df.discrete
            self.weighted_ = df.weighted
            if self.weighted_:
                algebra = df.algebra
            if bool(df):
                self.df_ = df.df
        else:
            df, self.weighted_ = load_instantaneous_df(df, no_duplicates=no_duplicates, weighted=weighted, keys=['u', 'v'], merge_function=merge_function)
            if df is not None:
                self.df_ = df
                self.sort_by = sort_by
                    
                if self.df_['ts'].dtype.kind != 'i' and self.df_['tf'].dtype.kind != 'i':
                    if discrete is None:
                        discrete = (True if self.df_.empty else False)
                    elif discrete:
                        warn('SemanticWarning: For a discrete instance time-instants should be an integers')
                elif discrete is None:
                    discrete = True
                self.discrete_ = True if discrete is None else discrete
                if self.weighted_:
                    self.algebra = make_algebra(operation_functions)
            else:
                self.discrete_ = True if discrete is None else discrete


    def __bool__(self):
        return hasattr(self, 'df_') and not self.df_.empty

    @property
    def weighted(self):
        return self.weighted_

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
    def sort_by(self, val):
        if not (hasattr(self, 'sort_by_') and self.sort_by_ == val):
            self.sorted_ = False
            self.sort_by_ = val

    @property
    def is_sorted_(self):
        return (hasattr(self, 'sort_by_') and hasattr(self, 'sorted_') and self.sorted_) or self.sort_by_ is None

    @property
    def sort_(self):
        if not self.is_sorted_:
            self.df_.sort_values(by=self.sort_by, inplace=True)
        return self

    def _init_base_class(self, df=None):
        if df is None:
            return self.__class__(weighted=self.weighted, discrete=self.discrete)
        else:
            if self.weighted:
                return self.__class__(df, weighted=self.weighted, discrete=self.discrete, operation_functions=self.algebra, merge_function=df.merge_function)
            else:
                return self.__class__(df, weighted=self.weighted, discrete=self.discrete)

    @property
    def sorted_df(self):
        if bool(self):
            return self.sort_.df_
        else:
            return self._init_base_class()

    def sort_df(self, sort_by):
        if self.sort_by is None:
            self.sort_by = sort_by
            return self.sort_df(sort_by)
        elif self.sort_by == sort_by:
            return self.sorted_df
        else:
            return self.df_.sort_values(by=self.sort_by, inplace=True)

    @property
    def df(self):
        if bool(self):
            return self.df_
        else:
            return self._init_base_class()

    @property
    def linkset(self):
        if bool(self):
            return LinkSetDF(self.df_.drop(columns=['ts']), no_duplicates=False, weighted=self.weighted)
        else:
            return LinkSetDF()

    @property
    def nodeset(self):
        if bool(self):
            nodes = self.df.v.drop_duplicates().append(
                self.df.u.drop_duplicates(),
                ignore_index=True).drop_duplicates().values
            return NodeSetS(nodes.flat)
        else:
            return NodeSetS()

    @property
    def basic_temporal_nodeset(self):
        # Create node stream
        if bool(self):
            return TemporalNodeSetB(self.nodeset, TimeSetDF([(self.df_.ts.min(), self.df_.ts.max())], discrete=self.discrete))
        else:
            return TemporalNodeSetB(discrete=self.discrete)

    @property
    def minimal_temporal_nodeset(self):
        # All the time intervals that a node belongs to a link
        if bool(self):
            mdf = self.df_[['v', 'ts']].rename(columns={'v': 'u'}).append(self.df_[['u', 'ts']])
            return ITemporalNodeSetDF(mdf, discrete=self.discrete)
        else:
            return ITemporalNodeSetDF(discrete=self.discrete)

    @property
    def timeset(self):
        if bool(self):
            return ITimeSetS(self.df.ts, discrete=self.discrete)
        else:
            return ITimeSetS(discrete=self.discrete)

    @property
    def number_of_interactions(self):
        if bool(self):
            return self.df.shape[0]
        else:
            return 0

    @property
    def _weighted_number_of_interactions(self):
        return self.df.w.sum()

    def __contains__(self, v):
        assert isinstance(v, tuple) and len(v) == 3
        if not bool(self) or (v[0] is None and v[1] is None and v[2] is None):
            return False

        lpd = []
        if v[0] is not None:
            lpd.append(self.df.u == v[0])
        if v[1] is not None:
            lpd.append(self.df.v == v[1])
        if v[2] is not None:
            if isinstance(v[2], Real):
                lpd.append(self.df_.index_at(v[2]))
            else:
                raise ValueError('Input can either be a real number or an ascending interval of two real numbers')
        return reduce(operator.__and__, lpd).any()

    def _duration_of_discrete(self, l=None, direction='out'):
        if not bool(self):
            if l is None:
                return LinkCollection()
            else:
                return .0
        if l is None:
            di = True
            if direction == 'out':
                def key(u, v):
                    return (u, v)
            elif direction == 'in':
                def key(u, v):
                    return (v, u)
            elif direction == 'both':
                def key(u, v):
                    return tuple(sorted([u, v]))
                di = False
            else:
                raise UnrecognizedDirection()
            return LinkCollection(Counter((key(a[0], a[1]) for a in iter(self))))
        else:
            di = True
            u, v = l
            if direction == 'out':
                df = self.df[(self.df.u == u) & (self.df.v == v)]
            elif direction == 'in':
                df = self.df[(self.df.v == u) & (self.df.u == v)]
            elif direction == 'both':
                df, di = self.df[self.df.u.isin({u, v}) & self.df.v.isin({u, v})], False
            else:
                raise UnrecognizedDirection()
            return ITimeSetS(df.ts, discrete=True).size

    def __iter__(self):
        if bool(self):
            if self.weighted:
                return self.df.itertuples(weights=True)
            else:
                return self.df.itertuples()
        else:
            return iter([])

    def links_at(self, t=None):
        if not bool(self):
            if t is None:
                return iter()
            else:
                return LinkSetDF()
        else:
            if t is None:
                if self.weighted:
                    def generate(iter_):
                        def dump(ct):
                            return list((k[0], k[1], v) for k, v in iteritems(ct))
                        prev = None
                        for u, v, ts, w in iter_:
                            if prev is None:
                                active_set, prev = Counter(), ts
                            elif ts != prev:
                                yield (prev, LinkSetDF(dump(active_set), no_duplicates=True, weighted=self.weighted))
                                active_set, prev = Counter(), ts
                            active_set[(u, v)] += w
                        if len(active_set):
                            yield (prev, LinkSetDF(dump(active_set), no_duplicates=True, weighted=self.weighted))
                else:
                    def generate(iter_):
                        prev = None
                        for u, v, ts in iter_:
                            if prev is None:
                                active_set, prev = {(u, v)}, ts
                            elif ts != prev:
                                yield (prev, LinkSetDF(list(active_set), no_duplicates=True, weighted=self.weighted))
                                active_set, prev = {(u, v)}, ts
                            else:
                                active_set.add((u, v))
                        if len(active_set):
                            yield (prev, LinkSetDF(list(active_set), no_duplicates=True, weighted=self.weighted))
                return TimeGenerator(generate(self.df.sort_values(by='ts').itertuples(name=None, index=False)), discrete=self.discrete, instantaneous=True)
            else:
                return LinkSetDF(self.df.df_at(t).drop(columns=['ts']), no_duplicates=False, weighted=self.weighted)

    def neighbors_at(self, u=None, t=None, direction='out'):
        if not bool(self):
            if u is None:
                return NodeCollection()
            if t is None:
                return TimeCollection(discrete=self.discrete, instantaneous=True)
            return NodeSetS()

        if u is None:
            if t is None:
                out = dict()
                if direction == 'out':
                    def add(d, u, v):
                        d[u].add(v)
                elif direction == 'in':
                    def add(d, u, v):
                        d[v].add(u)
                elif direction == 'both':
                    def add(d, u, v):
                        d[u].add(v)
                        d[v].add(u)
                else:
                    raise UnrecognizedDirection()

                prev = None
                for u, v, ts in self.df[['u', 'v', 'ts']].sort_values(by='ts').itertuples(name=None, index=False):
                    if prev is None:
                        cache = defaultdict(set)
                        prev = ts
                    elif ts != prev:
                        for u, s in iteritems(cache):
                            if u in out:
                                out[u].it.append((prev, NodeSetS(s)))
                            else:
                                out[u] = TimeCollection([(prev, NodeSetS(s))], instantaneous=False, discrete=self.discrete)
                        cache = defaultdict(set)
                        prev = ts
                    add(cache, u, v)
                for u, s in iteritems(cache):
                    if u in out:
                        out[u].it.append((prev, NodeSetS(s)))
                    else:
                        out[u] = TimeCollection([(prev, NodeSetS(s))], instantaneous=False, discrete=self.discrete)

                return NodeCollection(out)
            else:
                return LinkSetDF(self.df.df_at(t).drop(columns=['ts']), weighted=self.weighted).neighbors_of(u=None, direction=direction)
        else:
            di = True
            if direction == 'out':
                df = self.df[self.df.u == u].drop(columns=['u']).rename(columns={'v': 'u'})
            elif direction == 'in':
                df = self.df[self.df.v == u].drop(columns=['v'])
            elif direction == 'both':
                df = self.df[self.df.u == u].drop(columns=['u']).rename(columns={'v': 'u'})
                df = df.append(self.df[self.df.v == u].drop(columns=['v']), ignore_index=True)
                di = False
            else:
                raise UnrecognizedDirection()
            if t is None:
                return ITemporalNodeSetDF(df[['u', 'ts']], no_duplicates=di, discrete=self.discrete).nodes_at(t=None)
            else:
                return NodeSetS(df.df_at(t).u.values.flat)

    def _m_at_unweighted(self, t):
        if t is None:
            return TimeCollection(sorted(list(iteritems(Counter(iter(self.df.ts))))), instantaneous=True, discrete=self.discrete)
        else:
            return self.links_at(t).size

    def _m_at_weighted(self, t):
        if t is None:
            ct = Counter()
            for ts, w in self.df[['ts', 'w']].itertuples(index=False, name=None, weights=True):
                ct[ts] += w
            return TimeCollection(sorted(iteritems(ct)), instantaneous=True, discrete=self.discrete)
        else:
            return self.links_at(t).weighted_size

    def _degree_of_discrete(self, u, direction):
        if u is None:
            if direction == 'out':
                iter_ = (u for u, v, ts in self.dfm.itertuples())
            elif direction == 'in':
                iter_ = (v for u, v, ts in self.dfm.itertuples())
            elif direction == 'both':
                iter_ = (u for a, b, c in self.dfm.itertuples() for u in [a, b])
            else:
                raise UnrecognizedDirection()
            return NodeCollection(Counter(iter_))
        else:
            if direction == 'out':
                df = self.df[self.df.u == u].drop(columns=['u']).rename(columns={'v': 'u'})
            elif direction == 'in':
                df = self.df[self.df.v == u].drop(columns=['v'])
            elif direction == 'both':
                df = self.df[self.df.u == u].drop(columns=['u']).rename(columns={'v': 'u'})
                df = df.append(self.df[self.df.v == u].drop(columns=['v']), ignore_index=True)
            else:
                raise UnrecognizedDirection()
            return ITemporalNodeSetDF(df, disjoint_intervals=False, discrete=self.discrete).number_of_interactions

    def _degree_at_unweighted(self, u=None, t=None, direction='out'):
        if not bool(self):
            if u is None:
                return NodeCollection()
            if t is None:
                return TimeCollection(discrete=self.discrete, instantaneous=True)
            return 0

        if u is None:
            if t is None:
                out = dict()
                if direction == 'out':
                    def add(d, u, v):
                        d[u].add(v)
                elif direction == 'in':
                    def add(d, u, v):
                        d[v].add(u)
                elif direction == 'both':
                    def add(d, u, v):
                        d[u].add(v)
                        d[v].add(u)
                else:
                    raise UnrecognizedDirection()

                prev = None
                for u, v, ts in self.df[['u', 'v', 'ts']].sort_values(by='ts').itertuples(name=None, index=False):
                    if prev is None:
                        cache = defaultdict(set)
                        prev = ts
                    elif ts != prev:
                        for u, s in iteritems(cache):
                            if u in out:
                                out[u].it.append((prev, len(s)))
                            else:
                                out[u] = TimeCollection([(prev, len(s))], discrete=self.discrete, instantaneous=True)
                        cache = defaultdict(set)
                        prev = ts
                    add(cache, u, v)
                for u, s in iteritems(cache):
                    if u in out:
                        out[u].it.append((prev, len(s)))
                    else:
                        out[u] = TimeCollection([(prev, len(s))], discrete=self.discrete, instantaneous=True)

                return NodeCollection(out)
            else:
                return LinkSetDF(self.df.df_at(t).drop(columns=['ts']), weighted=self.weighted).degree(u=None, direction=direction)
        else:
            if direction == 'out':
                df = self.df[self.df.u == u].drop(columns=['u']).rename(columns={'v': 'u'})
            elif direction == 'in':
                df = self.df[self.df.v == u].drop(columns=['v'])
            elif direction == 'both':
                df = self.df[self.df.u == u].drop(columns=['u']).rename(columns={'v': 'u'})
                df = df.append(self.df[self.df.v == u].drop(columns=['v']), ignore_index=True, merge=True)
            else:
                raise UnrecognizedDirection()
            if t is None:
                dt = defaultdict(set)
                for u, ts in df[['u', 'ts']].itertuples(name=None, index=False):
                    dt[ts].add(u)
                return TimeCollection(sorted(list((ts, len(us)) for ts, us in iteritems(dt))), discrete=self.discrete, instantaneous=True)
            else:
                return len(set(df.df_at(t).u.values.flat))

    def _degree_at_weighted(self, u, t, direction):
        if u is None:
            if t is None:
                out = dict()
                if direction == 'out':
                    def add(d, u, v, w):
                        d[u] += w
                elif direction == 'in':
                    def add(d, u, v, w):
                        d[v] += w
                elif direction == 'both':
                    def add(d, u, v, w):
                        d[u] += w
                        d[v] += w
                else:
                    raise UnrecognizedDirection()

                prev = None
                for u, v, ts, w in self.df.sort_values(by='ts')[['u', 'v', 'ts', 'w']].itertuples(name=None, index=False):
                    if prev is None:
                        cache = Counter()
                        prev = ts
                    elif ts != prev:
                        for u, weight in iteritems(cache):
                            if u in out:
                                out[u].it.append((prev, weight))
                            else:
                                out[u] = TimeCollection([(prev, weight)], discrete=self.discrete, instantaneous=True)
                        cache = Counter()
                        prev = ts
                    add(cache, u, v, w)

                for u, weight in iteritems(cache):
                    if u in out:
                        out[u].it.append((prev, weight))
                    else:
                        out[u] = TimeCollection([(prev, weight)], discrete=self.discrete, instantaneous=True)

                return NodeCollection(out)
            else:
                return LinkSetDF(self.df.df_at(t).drop(columns=['ts']), weighted=self.weighted).degree(u=None, direction=direction)
        else:
            di = True
            if direction == 'out':
                df = self.df[self.df.u == u].drop(columns=['u']).rename(columns={'v': 'u'})
            elif direction == 'in':
                df = self.df[self.df.v == u].drop(columns=['v'])
            elif direction == 'both':
                df = self.df[self.df.u == u].drop(columns=['u']).rename(columns={'v': 'u'})
                df = df.append(self.df[self.df.v == u].drop(columns=['v']), ignore_index=True)
                di = False
            else:
                raise UnrecognizedDirection()
            if t is None:
                dt = Counter()
                for ts, w in df[['ts', 'w']].itertuples(weights=True):
                    dt[ts] += w
                return TimeCollection(sorted(list(iteritems(dt))), discrete=self.discrete, instantaneous=True)
            else:
                return df.df_at(t).w.sum()

    def times_of(self, l=None, direction='out'):
        if not bool(self):
            if l is None:
                return {}
            else:
                return TimeSetDF()

        if l is None:
            if direction == 'out':
                def key(u, v):
                    return (u, v)
            elif direction == 'in':
                def key(u, v):
                    return (v, u)
            elif direction == 'both':
                def key(u, v):
                    return tuple(sorted([u, v]))
            else:
                raise UnrecognizedDirection()
            times = defaultdict(set)
            for u, v, ts in self.df.itertuples(index=False, name=None):
                times[key(u, v)].add(ts)
            return LinkCollection({l: ITimeSetS(ts, discrete=self.discrete) for l, ts in iteritems(times)})
        else:
            di = True
            u, v = l
            if direction == 'out':
                df = self.df[(self.df.u == u) & (self.df.v == v)]
            elif direction == 'in':
                df = self.df[(self.df.v == u) & (self.df.u == v)]
            elif direction == 'both':
                df, di = self.df[self.df.u.isin({u, v}) & self.df.v.isin({u, v})], False
            else:
                raise UnrecognizedDirection()
            return ITimeSetS(df['ts'].values.flat, discrete=self.discrete)

    def neighbors_of(self, u=None, direction='out'):
        if not bool(self):
            if u is None:
                return {}
            else:
                return ITemporalNodeSetDF()

        if u is None:
            neighbors = defaultdict(set)
            if direction == 'out':
                def add(u, v, ts):
                    neighbors[u].add((v, ts))
            elif direction == 'in':
                def add(u, v, ts):
                    neighbors[v].add((u, ts))
            elif direction == 'both':
                def add(u, v, ts):
                    neighbors[u].add((v, ts))
                    neighbors[v].add((u, ts))
            else:
                raise UnrecognizedDirection()
            for u, v, ts in self.df.itertuples():
                add(u, v, ts)
            return NodeCollection({u: ITemporalNodeSetDF(ns) for u, ns in iteritems(neighbors)})
        else:
            if direction == 'out':
                df = self.df[self.df.u == u].drop(columns=['u']).rename(columns={'v': 'u'})
            elif direction == 'in':
                df = self.df[self.df.v == u].drop(columns=['v'])
            elif direction=='both':
                df = self.df[self.df.u == u].drop(columns=['u']).rename(columns={'v': 'u'})
                df = df.append(self.df[self.df.v == u].drop(columns=['v']), ignore_index=True)
            else:
                raise UnrecognizedDirection()
            return ITemporalNodeSetDF(df, no_duplicates=False, discrete=self.discrete)

    def substream(self, nsu=None, nsv=None, ts=None):
        if nsu is not None:
            if not isinstance(nsu, ABC.NodeSet):
                try:
                    nsu = NodeSetS(nsu)
                except Exception:
                    raise UnrecognizedNodeSet('nsu')
        if nsv is not None:
            if not isinstance(nsv, ABC.NodeSet):
                try:
                    nsv = NodeSetS(nsv)
                except Exception:
                    raise UnrecognizedNodeSet('nsv')
        if ts is not None:
            if not isinstance(ts, ABC.TimeSet):
                try:
                    if isinstance(ts, Iterable) and any(isinstance(t, Iterable) for t in ts):
                        ts = TimeSetDF(ts)
                    else:
                        ts = ITimeSetS(ts)
                except Exception:
                    raise UnrecognizedTimeSet('ts')

        if all(o is None for o in [nsu, nsv, ts]):
            return self.copy()
        if bool(self) and all((o is None or bool(o)) for o in [nsu, nsv, ts]):
            if nsu is not None and nsv is not None:
                df = self.df[self.df.u.isin(nsu) & self.df.v.isin(nsv)]
            elif nsu is not None:
                df = self.df[self.df.u.isin(nsu)]
            elif nsv is not None:
                df = self.df[self.df.v.isin(nsv)]
            else:
                df = self.df

            if ts is not None:
                if ts.instantaneous:
                    if self.weighted:
                        df = df.intersection(set(ts), by_key=False, on_column=['u', 'v'], intersection_function='unweighted')
                    else:
                        df = df.intersection(set(ts), by_key=False, on_column=['u', 'v'])
                else:
                    ts = list(ts_to_df(ts).sort_values(by='ts').itertuples(index=False, name=None))
                    df = [key for key in iter(self) if t_in(ts, key[2], 0, len(ts) - 1)]
            return self._init_base_class(df)
        else:
            return self._init_base_class()

    def __and__(self, ls):
        if isinstance(ls, ABC.TemporalLinkSet):
            if bool(ls) and bool(self):
                assert self.discrete == ls.discrete
                if isinstance(ls, ABC.ITemporalLinkSet):
                    if not isinstance(ls, self.__class__):
                        try:
                            return ls & self
                        except NotImplementedError:
                            ls = self._init_base_class(ls)
                    out = (self.df.intersection(ls.df, intersection_function=self.algebra['i']) if self.weighted else self.df.intersection(ls.df))
                    return self._init_base_class(out)
                else:
                    if self.weighted:
                        df = TemporalLinkSetDF(self, discrete=self.discrete, weighted=self.weighted, operation_functions=self.algebra) & ls
                    else:
                        df = TemporalLinkSetDF(self, discrete=self.discrete, weighted=self.weighted) & ls
                    return self._init_base_class(df.df.drop(columns=['tf']))
        else:
            raise UnrecognizedTemporalLinkSet('right operand')
        return TemporalLinkSetDF()

    def __or__(self, ls):
        if isinstance(ls, ABC.TemporalLinkSet):
            if not bool(self):
                return ls.copy()
            elif bool(ls):
                assert ls.discrete == self.discrete
                if isinstance(ls, ABC.ITemporalLinkSet):
                    if not isinstance(ls, self.__class__):
                        try:
                            return ls | self
                        except NotImplementedError:
                            ls = self._init_base_class(ls)
                    if self.weighted:
                        out = self._init_base_class(self.df.union(ls.df, union_function=self.algebra['u']))
                    else:
                        out = self._init_base_class(self.df.union(ls.df))
                    return self.__class__(out)
                else:
                    if self.weighted:
                        df = TemporalLinkSetDF(self, discrete=self.discrete, weighted=self.weighted, operation_functions=self.algebra) | ls
                    else:
                        df = TemporalLinkSetDF(self, discrete=self.discrete, weighted=self.weighted)
                    return df
            else:
                return self.copy()
        else:
            raise UnrecognizedTemporalLinkSet('right operand')

    def __sub__(self, ls):
        if isinstance(ls, ABC.TemporalLinkSet):
            if bool(self) and bool(ls):
                assert self.discrete == ls.discrete
                if isinstance(ls, ABC.ITemporalLinkSet):
                    if not isinstance(ls, self.__class__):
                        try:
                            return ls.__rsub__(self)
                        except (AttributeError, NotImplementedError):
                            ls = self._init_base_class(ls)
                    if self.weighted:
                        return self._init_base_class(self.df.difference(ls.df, difference_function=self.algebra['d']))
                    else:
                        return self._init_base_class(self.df.difference(ls.df))
                else:
                    return TemporalLinkSetDF(self) - ls
        else:
            raise UnrecognizedTemporalLinkSet('right operand')
        return self.copy()

    def issuperset(self, ls):
        if isinstance(ls, ABC.TemporalLinkSet):
            if bool(self) and bool(ls):
                assert self.discrete == ls.discrete
                if isinstance(ls, ABC.ITemporalLinkSet):
                    if not isinstance(ls, self.__class__):
                        try:
                            return ls.__issubset__(self)
                        except (AttributeError, NotImplementedError):
                            ls = self.__class__(ls, discrete=self.discrete, weighted=self.weighted)
                    return self.df.issuper(ls.df)
                else:
                    return TemporalLinkSetDF(self, discrete=self.discrete, weighted=self.weighted).issuper(ls)
            else:
                return not bool(ls)
        else:
            raise UnrecognizedTemporalLinkSet('ls')
        return False

    def temporal_neighborhood(self, ns, direction='out'):
        # if df join on u / combine (intersect) and the union intervals (for union)
        # if range
        derror = False
        if not isinstance(ns, ABC.TemporalNodeSet):
            raise UnrecognizedTemporalNodeSet('ns')
        assert self.discrete == ns.discrete
        cidf_ = class_interval_df(discrete=self.discrete, weighted=self.weighted)
        if isinstance(ns, TemporalNodeSetB):
            if direction == 'out':
                df = self.df.rename(columns={'v': 'u', 'u': 'v'})
                df = df[df.v.isin(ns.nodeset_)].drop('v', axis=1)
            elif direction == 'in':
                df = self.df[self.df.v.isin(ns.nodeset_)].drop('v', axis=1)
            elif direction == 'both':
                df = self.df.rename(columns={'v': 'u', 'u': 'v'})
                df = df[df.v.isin(ns.nodeset_)].drop('v', axis=1)
                df = df.append(self.df[self.df.v.isin(ns.nodeset_)].drop('v', axis=1))
            else:
                derror = True
            if not derror:
                ts = ns.timeset_
                if isinstance(ts, ABC.ITimeSet):
                    ts = its_to_idf(ts)
                else:
                    df, ts = cidf_(self.df), ts_to_df(ts)
                df = df.intersection(df, ts, on_columns=['u', 'v'], by_key=False)
        else:
            if isinstance(ns, ABC.ITemporalNodeSet):
                df, base_df = self.df, ins_to_idf(ns)
            else:
                df, base_df = cidf_(self.df), tns_to_df(ns)
            if direction == 'out':
                df = df.map_intersection(base_df)
            elif direction == 'in':
                df = df.rename(columns={'u': 'v', 'v': 'u'}).map_intersection(base_df)
            elif direction == 'both':
                dfo, df = df, df.map_intersection(base_df)
                df = df.append(dfo.rename(columns={'u': 'v', 'v': 'u'}).map_intersection(base_df), ignore_index=True)
            else:
                derror = True
        if derror:
            raise UnrecognizedDirection()
        if isinstance(df, cidf_):
            df = df.drop(columns=['tf'])
        return ITemporalNodeSetDF(df, no_duplicates=False, discrete=self.discrete)

    def induced_substream(self, ns):
        if isinstance(ns, ABC.TemporalNodeSet):
            if bool(self) and bool(ns):
                assert self.discrete == ns.discrete
                idf_ = class_interval_df(discrete=self.discrete, weighted=self.weighted)
                if isinstance(ns, TemporalNodeSetB):
                    tdf, ts = self.df_[self.df_['v'].isin(ns.nodeset_) & self.df_['u'].isin(ns.nodeset_)], ns.timeset_
                    if isinstance(ts, ABC.ITimeSet):
                        ts = its_to_idf(ts)
                    else:
                        df, ts = idf_(self.df), ts_to_df(ts)
                    tdf = tdf.intersection(ts, on_columns=['u', 'v'], by_key=False)
                else:
                    if isinstance(ns, ABC.ITemporalNodeSet):
                        df, base_df = self.df, ins_to_idf(ns)
                    else:
                        df, base_df = idf_(self.df), tns_to_df(ns)
                    if self.weighted:
                        tdf = df.cartesian_intersection(base_df, cartesian_intersection_function='unweighted')
                    else:
                        tdf = df.cartesian_intersection(base_df)
                if not tdf.empty:
                    if isinstance(tdf, idf_):
                        tdf = tdf.drop(columns=['tf'])
                return self.__class__(tdf, discrete=self.discrete, weighted=self.weighted)
        else:
            raise UnrecognizedTemporalNodeSet('ns')
        return self.__class__(discrete=self.discrete, weighted=self.weighted)

    def get_maximal_cliques(self, delta, direction='both'):
        df = DF((self.df.drop(columns='w') if self.weighted else self.df.copy()))
        di = (delta == .0)
        if not di:
            if self.instantaneous:
                min_time, max_time = df.ts.min(), df.ts.max()
            else:
                min_time, max_time = df.ts.min(), df.tf.max()
            df['ts'] -= delta/2.0
            df['tf'] = df['ts'] + delta
            df['ts'].clip(lower=min_time, inplace=True)
            df['tf'].clip(upper=max_time, inplace=True)
        if self.discrete:
            df = df.astype({'ts': int, 'tf': int})
        else:
            df['s'] = True
            df['f'] = True

        return TemporalLinkSetDF(df, disjoint_intervals=di, discrete=self.discrete, weighted=False).get_maximal_cliques(direction=direction)

    def ego_betweeness(self, u=None, t=None, direction='both', detailed=False):
        df = self.sort_df(sort_by=['ts'])
        both = direction == 'both'
        df = (df.rename(columns={'u': 'v', 'v': 'u'}) if direction == 'in' else df)
        lines = list(key for key in df[['u', 'v', 'ts']].itertuples(index=False, name=None))
        if u is None:
            neigh = {u: n.nodes_ for u, n in self.linkset.neighbors_of(direction=direction)}
            if t is None:
                return NodeCollection({u: functions.ego(u, neigh.get(u, set()), lines, both, detailed, self.discrete) for u in self.nodeset})
            else:
                return NodeCollection({u: functions.ego_at(u, neigh.get(u, set()), lines, t, both, detailed, self.discrete) for u in self.nodeset})
        elif t is None:
            return functions.ego(u, self.linkset.neighbors_of(u, direction=direction).nodes_, lines, both, detailed, self.discrete)
        else:
            return functions.ego_at(u, self.linkset.neighbors_of(u, direction=direction).nodes_, lines, t, both, detailed, self.discrete)


    def closeness(self, u=None, t=None, direction='both', detailed=False):
        from stream_graph._c_functions import closeness_c
        assert self.df_['ts'].dtype.kind == 'i'
        df = self.sort_df(sort_by=['ts'])
        both = direction == 'both'
        df = (df.rename(columns={'u': 'v', 'v': 'u'}) if direction == 'in' else df)
        return closeness_c(u, t, df[['u', 'v', 'ts']], both, detailed, discrete=self.discrete)

    def _to_discrete(self, bins, bin_size):
        df, bins = time_discretizer_df(self.df, bins, bin_size, columns=['ts'])
        return self.__class__(df, no_duplicates=False, discrete=True), bins
