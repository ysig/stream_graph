from __future__ import absolute_import
import operator
from functools import reduce
from numbers import Real
from collections import defaultdict
from collections import Counter
from collections import Iterable
from warnings import warn
from six import iteritems

import pandas as pd

from . import utils
from . import functions
from stream_graph import ABC
from .itemporal_node_set_df import ITemporalNodeSetDF
from .link_set_df import LinkSetDF
from .instantaneous_df import InstantaneousDF
from .instantaneous_df import InstantaneousWDF
from .interval_df import IntervalDF
from .interval_df import IntervalWDF
from .node_set_s import NodeSetS
from .temporal_node_set_b import TemporalNodeSetB
from .temporal_link_set_df import TemporalLinkSetDF
from .itime_set_s import ITimeSetS
from .time_set_df import TimeSetDF
from stream_graph.collections import NodeCollection
from stream_graph.collections import LinkCollection
from stream_graph.collections import TimeGenerator
from stream_graph.collections import TimeCollection
from stream_graph.collections import TimeSparseCollection
from stream_graph.exceptions import UnrecognizedTemporalLinkSet
from stream_graph.exceptions import UnrecognizedTemporalNodeSet
from stream_graph.exceptions import UnrecognizedNodeSet
from stream_graph.exceptions import UnrecognizedTimeSet
from stream_graph.exceptions import UnrecognizedDirection

class ITemporalLinkSetDF(ABC.ITemporalLinkSet):
    """DataFrame implementation of ABC.ITEMPORALLINKSET"""
    def __init__(self, df=None, no_duplicates=True, sort_by=None, discrete=None, weighted=False):
        """Initialize an instantaneous Temporal Link Set.

        Parameters
        ----------
        df: pandas.DataFrame or Iterable, default=None
            If a DataFrame it should contain three columns for u and v and ts.
            If an Iterable it should produce :code:`(u, v, ts)` tuples of two NodeId (int or str) and one timestamps (Real) with :code:`ts`.

        no_duplicates: Bool, default=False
            Defines if for each link there are no duplicate timestamps.

        sort_by: Any non-empty subset of ['u', 'v', 'ts'].
            The order of the DataFrame elements by which they will be produced when iterated.

        """
        is_df = isinstance(df, pd.DataFrame)
        if not ((df is None) or (is_df and df.empty) or (not is_df and not bool(df))):
            #import pdb; pdb.set_trace()
            class_ = (InstantaneousWDF if weighted else InstantaneousDF)
            if not isinstance(df, InstantaneousDF):
                if not is_df:
                    if isinstance(df, ABC.TemporalLinkSet):
                        discrete = df.discrete
                    df = pd.DataFrame(list(iter(df)))
                if len(df.columns) == 3:
                    try:
                        df = df[['u', 'v', 'ts']]
                    except Exception:
                        df.columns = ['u', 'v', 'ts']                            
                elif len(df.columns) == 4 :
                    try:
                        df = df[['u', 'v', 'ts', 'w']]
                    except Exception:
                        df.columns = ['u', 'v', 'ts', 'w']
                    if not weighted:
                        df.drop(columns='w')
                else:
                    raise ValueError('An Iterable input should consist of either 3 or 4 elements')
                df = class_(df)
            self.df_ = df
            self.sort_by = sort_by
            self.merged_ = no_duplicates
            self.weighted_ = weighted
            if discrete is None:
                discrete = False
            self.discrete_ = discrete
            if discrete and self.df_['ts'].dtype.kind != 'i' and self.df_['tf'].dtype.kind != 'i':
                warn('SemanticWarning: For a discrete instance time-instants should be an integers')

    def __bool__(self):
        return hasattr(self, 'df_') and not self.df_.empty

    @property
    def weighted(self):
        if bool(self):
            return self.weighted_
        else:
            return None

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
    def sort_by(self, val):
        if not (hasattr(self, 'sort_by_') and self.sort_by_ == val):
            self.sorted_ = False
            self.sort_by_ = val

    @property
    def is_merged_(self):
        return hasattr(self, 'merged_') and self.merged_

    @property
    def is_sorted_(self):
        return (hasattr(self, 'sort_by_') and hasattr(self, 'sorted_') and self.sorted_) or self.sort_by_ is None

    @property
    def sort_(self):
        if not self.is_sorted_:
            self.df_.sort_values(by=self.sort_by, inplace=True)
        return self

    @property
    def merge_(self):
        if not self.is_merged_:
            self.df_.merge(inplace=True)
            self.merged_ = True
        return self

    @property
    def df(self):
        if bool(self):
            return self.merge_.sort_.df_
        else:
            return InstantaneousDF(columns=['u', 'v', 'ts'])

    @property
    def dfm(self):
        if bool(self):
            return self.merge_.df_
        else:
            return InstantaneousDF(columns=['u', 'v', 'ts'])

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
            return TemporalNodeSetB()

    @property
    def minimal_temporal_nodeset(self):
        # All the time intervals that a node belongs to a link
        if bool(self):
            mdf = self.df_[['v', 'ts']].rename(columns={'v': 'u'}).append(self.df_[['u', 'ts']])
            return ITemporalNodeSetDF(mdf, no_duplicates=False, discrete=self.discrete)
        else:
            return ITemporalNodeSetDF()

    @property
    def timeset(self):
        if bool(self):
            return ITimeSetS(self.df_[['ts']].values.flat, discrete=self.discrete)
        else:
            return ITimeSetS()

    @property
    def _size_discrete(self):
        return self.dfm.shape[0]

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
            times = defaultdict(float)
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
            return LinkCollection(Counter((key(u, v) for u, v, _ in iter(self))))
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
            return ITimeSetS(df.ts.values.flat, discrete=True).size            

    def __iter__(self):
        if bool(self):
            if self.weighted:
                return self.df.itertuples(index=False, name=None, weights=True)
            else:
                return self.df.itertuples(index=False, name=None)
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
                return TimeGenerator(generate(self.dfm.sort_values(by='ts').itertuples(name=None, index=False)), True)
            else:
                return LinkSetDF(self.df.df_at(t).drop(columns=['ts']), no_duplicates=False, weighted=self.weighted)

    def neighbors_at(self, u=None, t=None, direction='out'):
        if not bool(self):
            if u is None:
                return NodeCollection()
            if t is None:
                return TimeCollection()
            return NodeSetS()

        if u is None:
            if t is None:
                out = defaultdict(list)
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
                for u, v, ts in self.df_[['u', 'v', 'ts']].sort_values(by='ts').itertuples(name=None, index=False):
                    if prev is None:
                        cache = defaultdict(set)
                        prev = ts
                    elif ts != prev:
                        for u, s in iteritems(cache):
                            out[u].append((prev, NodeSetS(s)))
                        cache = defaultdict(set)
                        prev = ts
                    add(cache, u, v)
                for u, s in iteritems(cache):
                    out[u].append((prev, NodeSetS(s)))

                for u in out.keys():
                    out[u] = TimeCollection(out[u])

                return NodeCollection(out)
            else:
                return LinkSetDF(self.dfm.df_at(t).drop(columns=['ts']), weighted=self.weighted).neighbors(u=None, direction=direction)
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
            return TimeCollection(sorted(list(iteritems(Counter(iter(self.df.ts))))), True)
        else:
            return self.links_at(t).size

    def _m_at_weighted(self, t):
        if t is None:
            ct = Counter()
            for ts, w in self.df_[['ts', 'w']].itertuples(index=False, name=None, weights=True):
                ct[ts] += w
            return TimeCollection(sorted(iteritems(ct)), True)
        else:
            return self.links_at(t).size_weighted

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
            elif direction=='both':
                df = self.df[self.df.u == u].drop(columns=['u']).rename(columns={'v': 'u'})
                df = df.append(self.df[self.df.v == u].drop(columns=['v']), ignore_index=True)
            else:
                raise UnrecognizedDirection()
            return TemporalNodeSetDF(df, disjoint_intervals=False, discrete=self.discrete).size

    def _degree_at_unweighted(self, u=None, t=None, direction='out'):
        if not bool(self):
            if u is None:
                return NodeCollection()
            if t is None:
                return TimeCollection()
            return 0

        if u is None:
            if t is None:
                out = defaultdict(list)
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
                for u, v, ts in self.df_[['u', 'v', 'ts']].sort_values(by='ts').itertuples(name=None, index=False):
                    if prev is None:
                        cache = defaultdict(set)
                        prev = ts
                    elif ts != prev:
                        for u, s in iteritems(cache):
                            out[u].append((prev, len(s)))
                        cache = defaultdict(set)
                        prev = ts
                    add(cache, u, v)
                for u, s in iteritems(cache):
                    out[u].append((prev, len(s)))

                for u in out.keys():
                    out[u] = TimeCollection(out[u], True)

                return NodeCollection(out)
            else:
                return LinkSetDF(self.dfm.df_at(t).drop(columns=['ts']), weighted=self.weighted).degree(u=None, direction=direction)
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
                dt = defaultdict(set)
                for u, ts in df[['u', 'ts']].itertuples(name=None, index=False):
                    dt[ts].add(u)
                return TimeCollection(sorted(list((ts, len(us)) for ts, us in iteritems(dt))), True) 
            else:
                return len(set(df.df_at(t).u.values.flat))

    def _degree_at_weighted(self, u, t, direction):
        if u is None:
            if t is None:
                out = defaultdict(list)
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
                for u, v, ts, w in self.df_.sort_values(by='ts')[['u', 'v', 'ts', 'w']].itertuples(name=None, index=False):
                    if prev is None:
                        cache = Counter()
                        prev = ts
                    elif ts != prev:
                        for u, weight in iteritems(cache):
                            out[u].append((prev, weight))
                        cache = Counter()
                        prev = ts 
                    add(cache, u, v, w)
                for u, weight in iteritems(cache):
                    out[u].append((prev, weight))

                for u in out.keys():
                    out[u] = TimeCollection(out[u], True)
                return NodeCollection(out)
            else:
                return LinkSetDF(self.dfm.df_at(t).drop(columns=['ts']), weighted=self.weighted).degree(u=None, direction=direction)
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
                for ts, w in df[['ts', 'w']].itertuples(name=None, index=False, weights=True):
                    dt[ts] += w
                return TimeCollection(sorted(list(iteritems(dt))), True)
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
            for u, v, ts in self.dfm.itertuples(index=False, name=None):
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

    def neighbors(self, u=None, direction='out'):
        if not bool(self):
            if u is None:
                return {}
            else:
                return TemporalNodeSetDF()

        if u is None:
            neighbors = defaultdict(set)
            if direction == 'out':
                def add(u, v, ts):
                    neighbors[u].add((v, ts))
            elif direction == 'in':
                def add(u, v, ts):
                    neighbors[v].add((u, ts))
            elif direction=='both':
                def add(u, v, ts):
                    neighbors[u].add((v, ts))
                    neighbors[v].add((u, ts))
            else:
                raise UnrecognizedDirection()
            for u, v, ts in self.dfm.itertuples(index=False, name=None):
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
                    df = df.intersect(set(ts), by_key=False, on_column=['u', 'v'])
                else:
                    ts = list(utils.ts_to_df(ts).sort_values(by='ts').itertuples(index=False, name=None))
                    df = [key for key in iter(self) if utils.t_in(ts, key[2], 0, len(ts) - 1)]
            return self.__class__(df, discrete=self.discrete, weighted=self.weighted)
        else:
            return self.__class__()

    def __and__(self, ls):
        if isinstance(ls, ABC.TemporalLinkSet):
            if bool(ls) and bool(self):
                assert self.discrete == ls.discrete
                if isinstance(ls, ABC.ITemporalLinkSet):
                    if not isinstance(ls, self.__class__):
                        try:
                            return ls & self
                        except NotImplementedError:
                            ls = self.__class__(ls, discrete=self.discrete, weighted=self.weighted)
                    return self.__class__(self.df.intersect(ls.df), weighted=self.weighted, discrete=self.discrete)
                else:
                    df = TemporalLinkSetDF(self, discrete=self.discrete, weighted=self.weighted) & ls
                    df = TemporalLinkSetDF(df, discrete=self.discrete, weighted=self.weighted).df.drop(columns=['tf'])
                    return self.__class__(df, discrete=self.discrete, weighted=self.weighted)
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
                            ls = self.__class__(ls, discrete=self.discrete, weighted=self.weighted)
                    return self.__class__(self.df.union(ls.df), discrete=self.discrete, weighted=self.weighted)
                else:
                    return TemporalLinkSetDF(self, discrete=self.discrete, weighted=self.weighted) | ls
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
                            ls = self.__class__(ls, discrete=self.discrete, weighted=self.weighted)
                    return self.__class__(self.df.difference(ls.df), discrete=self.discrete, weighted=self.weighted)
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

    def neighborhood(self, ns, direction='out'):
        # if df join on u / combine (intersect) and the union intervals (for union)
        # if range
        derror = False
        if not isinstance(ns, ABC.TemporalNodeSet):
            raise UnrecognizedTemporalNodeSet('ns')
        assert self.discrete == ns.discrete
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
                    ts = utils.its_to_idf(ts)
                else:
                    df, ts = IntervalDF(self.df), utils.ts_to_df(ts)
                df = df.intersect(df, ts, on_columns=['u', 'v'], by_key=False)
        else:
            if isinstance(ns, ABC.ITemporalNodeSet):
                df, base_df = self.dfm, utils.ins_to_idf(ns)
            else:
                df, base_df = IntervalDF(self.dfm), utils.ns_to_df(ns)
            if direction == 'out':
                df = df.map_intersect(base_df)
            elif direction == 'in':
                df = df.rename(columns={'u': 'v', 'v': 'u'}).map_intersect(base_df)
            elif direction == 'both':
                dfo, df = df, df.map_intersect(base_df)
                df = df.append(dfo.rename(columns={'u': 'v', 'v': 'u'}).map_intersect(base_df), ignore_index=True)
            else:
                derror = True
        if derror:
            raise UnrecognizedDirection()
        if isinstance(df, IntervalDF):
            df = df.drop(columns=['tf'])
        return ITemporalNodeSetDF(df, no_duplicates=False, discrete=self.discrete)

    def induced_substream(self, ns):
        if isinstance(ns, ABC.TemporalNodeSet):
            if bool(self) and bool(ns):
                assert self.discrete == ns.discrete
                idf_  = (IntervalDF if self.weighted else IntervalWDF)
                if isinstance(ns, TemporalNodeSetB):
                    tdf, ts = self.df_[self.df_['v'].isin(ns.nodeset_) & self.df_['u'].isin(ns.nodeset_)], ns.timeset_
                    if isinstance(ts, ABC.ITimeSet):
                        ts = utils.its_to_idf(ts)
                    else:
                        df, ts = idf_(self.df), utils.ts_to_df(ts)
                    tdf = tdf.intersect(ts, on_columns=['u', 'v'], by_key=False)
                else:
                    base_df = utils.ns_to_df(ns)
                    if isinstance(ns, ABC.ITemporalNodeSet):
                        df, base_df = self.dfm, utils.ins_to_idf(ns)
                    else:
                        df, base_df = idf_(self.dfm), utils.ns_to_df(ns)
                    tdf = self.df_.cartesian_intersect(base_df)
                if not tdf.empty:
                    if isinstance(tdf, idf_):
                        tdf = tdf.drop(columns=['tf'])   
                return self.__class__(tdf, discrete=self.discrete, weighted=self.weighted)
        else:
            raise UnrecognizedTemporalNodeSet('ns')
        return self.__class__()

    def get_maximal_cliques(self, delta, direction='both'):
        df = (self.df.drop(columns='w') if self.weighted else self.df.copy())
        di = (delta == .0)
        if not di:
            if self.instantaneous:
                min_time, max_time = df.ts.min(), df.ts.max()
            else:
                min_time, max_time = df.ts.min(), df.tf.max()
            df['ts'] -= delta/2.0
            df['tf'] = df['ts'] + delta
            df['ts'].clip_lower(min_time, inplace=True)
            df['tf'].clip_upper(max_time, inplace=True)
        if self.discrete:
            df = df.astype({'ts': int, 'tf': int})
        return TemporalLinkSetDF(df, disjoint_intervals=di, discrete=self.discrete, weighted=self.weighted).get_maximal_cliques(direction=direction)


    def ego_betweeness(self, u=None, t=None, direction='both', detailed=False):
        if direction == 'out':
            def as_link(u, v):
                return (u, v)
            def add_nodes(u, v):
                nodes[u].add(v)
        elif direction == 'in':
            def as_link(u, v):
                return (v, u)
            def add_nodes(u, v):
                nodes[v].add(u)
        elif direction == 'both':
            def as_link(u, v):
                return frozenset([u, v])
            def add_nodes(u, v):
                nodes[v].add(u)
                nodes[u].add(v)
        else:
            raise UnrecognizedDirection()

        if self.sort_by == ['ts']:
            df = self.df
        else:
            df = self.dfm.sort_values(by=['ts'])

        both = False
        if direction == 'in':
            df = df.rename(columns={'u': 'v', 'v': 'u'})
        elif direction == 'both':
            both = True

        lines = list(key for key in df[['u', 'v', 'ts']].itertuples(index=False, name=None))            
        if u is None:
            neigh = {u: n.nodes_ for u, n in self.linkset.neighbors(direction=direction)}
            if t is None:
                return NodeCollection({u: functions.ego(u, neigh.get(u, set()), lines, both, detailed) for u in self.nodeset})
            else:
                return NodeCollection({u: functions.ego_at(u, neigh.get(u, set()), lines, t, both, detailed) for u in self.nodeset})
        elif t is None:
            return functions.ego(u, self.linkset.neighbors(u, direction=direction).nodes_, lines, both, detailed)
        else:
            return functions.ego_at(u, self.linkset.neighbors(u, direction=direction).nodes_, lines, t, both, detailed)


    def closeness(self, u=None, t=None, direction='both', detailed=False):
        from stream_graph._c_functions import closeness_c
        assert self.df_['ts'].dtype.kind in ['i']

        if direction == 'in':
            def as_link(u, v):
                return (v, u)
            def add_nodes(u, v):
                nodes[v].add(u)
        elif direction == 'both':
            def as_link(u, v):
                return frozenset([u, v])
            def add_nodes(u, v):
                nodes[v].add(u)
                nodes[u].add(v)
        elif direction != 'out':
            raise UnrecognizedDirection()

        if self.sort_by == ['ts']:
            df = self.df
        else:
            df = self.dfm.sort_values(by=['ts'])

        both = False
        if direction == 'in':
            df = df.rename(columns={'u': 'v', 'v': 'u'})
        elif direction == 'both':
            both = True

        return closeness_c(u, t, df[['u', 'v', 'ts']], both, detailed)

    def _to_discrete(self, bins, bin_size):
        df, bins = time_discretizer_df(self.df, bins, bin_size, columns=['ts'])
        return self.__class__(df, no_duplicates=False, discrete=True), bins
