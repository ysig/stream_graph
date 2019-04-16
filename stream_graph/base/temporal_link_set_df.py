import operator
from functools import reduce
from warnings import warn
from numbers import Real
from collections import deque
from collections import defaultdict
from collections import Counter
from six import iteritems
from six import itervalues

import pandas as pd

from . import utils
from stream_graph import ABC
from .temporal_node_set_df import TemporalNodeSetDF
from .temporal_node_set_b import TemporalNodeSetB
from .link_set_df import LinkSetDF
from .time_set_df import TimeSetDF
from .interval_df import IntervalDF
from .interval_df import IntervalWDF
from .node_set_s import NodeSetS
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


class TemporalLinkSetDF(ABC.TemporalLinkSet):
    """DataFrame implementation of ABC.TemporalLinkSet"""
    def __init__(self, df=None, disjoint_intervals=True, sort_by=['u', 'v', 'ts', 'tf'], discrete=None, weighted=False):
        """Initialize a Temporal Link Set.

        Parameters
        ----------
        df: pandas.DataFrame or Iterable, default=None
            If a DataFrame it should contain four columns for u and v and ts, tf.
            If an Iterable it should produce :code:`(u, v, ts, tf)` tuples of two NodeId (int or str) and two timestamps (Real) with :code:`ts < tf`.

        disjoint_intervals: Bool, default=False
            Defines if for each link all intervals are disjoint.

        sort_by: Any non-empty subset of ['u', 'v', 'ts', 'tf'].
            The order of the DataFrame elements by which they will be produced when iterated.

        """
        is_df = isinstance(df, pd.DataFrame)
        if not ((df is None) or (is_df and df.empty) or (not is_df and not bool(df))):
            if isinstance(df, ABC.TemporalLinkSet):
                discrete = df.discrete
            elif discrete is None:
                discrete = False
            kargs = {}

            cl = (IntervalWDF if weighted else IntervalDF)
            if isinstance(df, ABC.ITemporalLinkSet):
                if not isinstance(df, ITemporalLinkSetDF):
                    df = list(iter(df))
                else:
                    df = df.df
                df = cl(df, columns = ['u', 'v', 'ts'] + (['w'] if df.weighted else []))
            elif not isinstance(df, cl):
                if not is_df:
                    df = pd.DataFrame(list(iter(df)))
                if len(df.columns) == 4:
                    df.columns = ['u', 'v', 'ts', 'tf']
                    if weighted and not disjoint_intervals:
                        df, disjoint_intervals = IntervalDF(df).merge(), True
                elif len(df.columns) == 5:
                    df.columns = ['u', 'v', 'ts', 'tf', 'w']
                else:
                    raise ValueError('An Iterable input should consist of either 4 or 5 elements')
                df = cl(df)
            self.df_ = df
            assert sorted(df.columns) == sorted(['u', 'v', 'ts', 'tf'] + (['w'] if weighted else []))
            self.sort_by = sort_by
            self.merged_ = disjoint_intervals
            if discrete and self.df_['ts'].dtype.kind != 'i' and self.df_['tf'].dtype.kind != 'i':
                warn('SemanticWarning: For a discrete instance time-instants should be an integers')
            self.discrete_ = discrete
            self.weighted_ = weighted

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
            self.sorted_ = True
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
        elif self.weigthed:
            return IntervalDF(columns=['u', 'v', 'ts', 'tf', 'w'])
        else:
            return IntervalDF(columns=['u', 'v', 'ts', 'tf'])

    @property
    def dfm(self):
        if bool(self):
            return self.merge_.df_
        elif self.weigthed:
            return IntervalDF(columns=['u', 'v', 'ts', 'tf', 'w'])
        else:
            return IntervalDF(columns=['u', 'v', 'ts', 'tf'])

    @property
    def size(self):
        if bool(self):
            return self.dfm.measure_time(discrete=self.discrete)
        else:
            return 0

    @property
    def basic_temporal_nodeset(self):
        # Create node stream
        if bool(self):
            return TemporalNodeSetB(self.nodeset, TimeSetDF([(self.df_.ts.min(), self.df_.tf.max())], discrete=self.discrete))
        else:
            return TemporalNodeSetB()

    @property
    def minimal_temporal_nodeset(self):
        # All the time intervals that a node belongs to a link
        if bool(self):
            mdf = self.df_[['v', 'ts', 'tf']].rename(columns={'v': 'u'}).append(self.df_[['u', 'ts', 'tf']])
            return TemporalNodeSetDF(mdf, disjoint_intervals=False, discrete=self.discrete)
        else:
            return TemporalNodeSetDF()

    @property
    def linkset(self):
        if bool(self):
            if self.weighted:
                return LinkSetDF(self.df_[['u', 'v', 'w']], no_duplicates=False, weighted=True)
            else:
                return LinkSetDF(self.df_[['u', 'v']].drop_duplicates())
        else:
            return LinkSetDF()

    @property
    def timeset(self):
        if bool(self):
            return TimeSetDF(self.df_[['ts', 'tf']], disjoint_intervals=False, discrete=self.discrete)
        else:
            return TimeSetDF()

    @property
    def nodeset(self):
        if bool(self):
            nodes = self.df.v.drop_duplicates().append(
                self.df.u.drop_duplicates(),
                ignore_index=True).drop_duplicates().values
            return NodeSetS(nodes.flat)
        else:
            return NodeSetS()

    def __bool__(self):
        return hasattr(self, 'df_') and not self.df_.empty

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
            t = v[2]
            if isinstance(t, tuple) and len(t) == 2 and isinstance(t[0], Real) and isinstance(t[1], Real) and t[0]<=t[1]:
                lpd.append(self.df_.index_at_interval(t[0], t[1]))
            elif isinstance(t, Real):
                lpd.append(self.df_.index_at(t))
            else:
                raise ValueError('Input can either be a real number or an ascending interval of two real numbers')
        return reduce(operator.__and__, lpd).any()

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

        if t is None:
            def generate(df, weighted):
                key = df.iloc[0]
                e, active_links = key[0], {tuple(key[2:])}
                for k in df.iloc[1:].itertuples(index=False, name=None):
                    t, f = k[:2]
                    key = k[2:]
                    if t > e:
                        ef, sal = e, set(active_links)
                        yield (ef, LinkSetDF(sal, weighted=weighted))
                    if f:
                        active_links.add(key)
                    else:
                        # finish
                        active_links.remove(key)
                    e = t
                if e != ef or sal != active_links:
                    yield (e, LinkSetDF(active_links, weighted=weighted))
            if self.weighted:
                return TimeGenerator(generate(self.dfm.events[['t', 'start', 'u', 'v', 'w']], True))
            else:
                return TimeGenerator(generate(self.dfm.events[['t', 'start', 'u', 'v']], False))
        elif isinstance(t, tuple) and len(t) == 2 and isinstance(t[0], Real) and isinstance(t[1], Real) and t[0]<=t[1]:
            return LinkSetDF(self.df.df_at_interval(t[0], t[1]).drop(columns=['ts', 'tf']), no_duplicates=False, weighted=self.weighted)
        else:
            return LinkSetDF(self.df.df_at(t).drop(columns=['ts', 'tf']), no_duplicates=False, weighted=self.weighted)

    def times_of(self, l=None, direction='out'):
        if not bool(self):
            if l is None:
                return LinkCollection()
            else:
                return TimeSetDF()
        if l is None:
            times = defaultdict(list)
            di = True
            if direction == 'out':
                def add(u, v, ts, tf):
                    times[(u, v)].append((ts, tf))
            elif direction == 'in':
                def add(u, v, ts, tf):
                    times[(u, v)].append((ts, tf))
            elif direction == 'both':
                def add(u, v, ts, tf):
                    times[tuple(sorted([u, v]))].append((ts, tf))
                di = False
            else:
                raise UnrecognizedDirection()
            for u, v, ts, tf in self.df.itertuples():
                add(u, v, ts, tf)
            return LinkCollection({l: TimeSetDF(ts, disjoint_intervals=di, discrete=self.discrete) for l, ts in iteritems(times)})
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
            return TimeSetDF(df.drop(columns=['u', 'v']+(['w'] if self.weighted else [])), disjoint_intervals=di, discrete=self.discrete)

    def duration_of(self, l=None, direction='out'):
        if not bool(self):
            if l is None:
                return LinkCollection()
            else:
                return .0
        if l is None:
            times = defaultdict(float)
            di = True
            dc = (1 if self.discrete else 0)
            if direction == 'out':
                def add(u, v, ts, tf):
                    times[(u, v)] += tf - ts + dc
            elif direction == 'in':
                def add(u, v, ts, tf):
                    times[(v, u)] += tf - ts + dc
            elif direction == 'both':
                def add(u, v, ts, tf):
                    times[tuple(sorted([u, v]))] += tf - ts + dc
                di = False
            else:
                raise UnrecognizedDirection()
            for u, v, ts, tf in self.dfm.itertuples():
                add(u, v, ts, tf)
            return LinkCollection({l: ts for l, ts in iteritems(times)})
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
            return TimeSetDF(df.drop(columns=['u', 'v']+(['w'] if self.weighted else [])), disjoint_intervals=di, discrete=self.discrete).size

    def neighbors_at(self, u=None, t=None, direction='out'):
        if not bool(self):
            if u is None:
                return NodeCollection()
            if t is None:
                return TimeCollection()
            return NodeSetS()

        if u is None:
            if t is None:
                df = self.dfm.events[['u', 'v', 't', 'start']]
                active_neighbors, e, out = defaultdict(set), defaultdict(float), defaultdict(list)
                if direction == 'out':
                    iter_ = df.itertuples(index=False, name=None)
                elif direction == 'in':
                    iter_ = ((v, u, t, f) for u, v, t, f in df.itertuples(index=False, name=None))
                elif direction == 'both':
                    iter_ = (it for u, v, t, f in df.itertuples(index=False, name=None) for it in [(u, v, t, f), (v, u, t, f)])
                else:
                    raise UnrecognizedDirection()            
                for u, v, t, f in iter_:
                    if f:
                        # start
                        if t > e[u] and e[u] != 0:
                            out[u].append((e[u], set(active_neighbors[u]), f))
                            active_neighbors[u] = set()
                        active_neighbors[u].add(v)
                    else:
                        # finish
                        if t > e[u]:
                            out[u].append((e[u], set(active_neighbors[u]), f))
                            active_neighbors[u] = set()
                        active_neighbors[u].add(v)
                    e[u] = t
                for u in out.keys():
                    if len(out[u]) or e[u] != out[u][-1][0] or active_neighbors[u] != out[u][-1][1]:
                        out[u].append((e[u], set(active_neighbors[u]), not out[u][-1][2]))
                    out[u] = TimeSparseCollection(out[u], caster=NodeSetS)
                return NodeCollection(out)
            else:
                return LinkSetDF(self.dfm.df_at(t)).neighbors(u=None, direction=direction)
        else:
            di, wc = True, (['w'] if self.weighted else [])
            if direction == 'out':
                df = self.df[self.df.u == u].drop(columns=['u']+wc).rename(columns={'v': 'u'})
            elif direction == 'in':
                df = self.df[self.df.v == u].drop(columns=['v']+wc)
            elif direction == 'both':
                df = self.df[self.df.u == u].drop(columns=['u']+wc).rename(columns={'v': 'u'})
                df = df.append(self.df[self.df.v == u].drop(columns=['v']+wc), ignore_index=True)
                di = False
            else:
                raise UnrecognizedDirection()
            if t is None:
                return TemporalNodeSetDF(df, disjoint_intervals=di).nodes_at(t=None)
            else:
                return NodeSetS(df[df.index_at(t)].u.values.flat)

    def _degree_at_weighted(self, u, t, direction):
        if u is None:
            if t is None:
                df = self.dfm.events[['u', 'v', 't', 'start', 'w']]
                active_nw, e, out = defaultdict(Counter), defaultdict(float), defaultdict(list)
                if direction == 'out':
                    iter_ = df.itertuples(index=False, name=None)
                elif direction == 'in':
                    iter_ = ((v, u, t, f, w) for u, v, t, f, w in df.itertuples(index=False, name=None))
                elif direction == 'both':
                    iter_ = (it for u, v, t, f, w in df.itertuples(index=False, name=None) for it in [(u, v, t, f, w), (v, u, t, f, w)])
                else:
                    raise UnrecognizedDirection()
                for u, v, t, f, w in iter_:
                    if f:
                        # start
                        if t > e[u] and e[u] != 0:
                            out[u].append((e[u], sum(itervalues(active_nw[u]))))
                        active_nw[u][v] += w
                    else:
                        # finish
                        if t > e[u]:
                            out[u].append((e[u], sum(itervalues(active_nw[u]))))
                        active_nw[u][v] -= w
                    e[u] = t
                for u in out.keys():
                    
                    if len(out[u]) or e[u] != out[u][-1][1] or out[u][-1][1] != sum(itervalues(active_nw[u])):
                        out[u].append((e[u], sum(itervalues(active_nw[u]))))
                    out[u] = TimeCollection(out[u])
                return NodeCollection(out)
            else:
                return LinkSetDF(self.df.df_at(t), weighted=True).degree(u=None, direction=direction, weights=True)
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
                if not di:
                    df.merge(inplace=True)
                df = df.events[['u', 't', 'start', 'w']]
                u, e, f, w = df.iloc[0]
                active_nodes, nodes = Counter(), list()
                active_nodes[u] += w
                for u, t, f, w in df.iloc[1:].itertuples(index=False, name=None):
                    if t > e:
                        nodes.append((e, sum(itervalues(active_nodes))))
                    active_nodes[u] += (w if f else -w)
                    e = t
                ef, nan = nodes[-1]
                if e != ef or nan != sum(itervalues(active_nodes)):
                    nodes.append((e, sum(itervalues(active_nodes))))
                return TimeCollection(nodes, False)
            else:
                return df.w[df.index_at(t)].sum()

    def _degree_at_unweighted(self, u, t, direction):
        if u is None:
            if t is None:
                df = self.dfm.events[['u', 'v', 't', 'start']]
                active_neighbors, e, out = defaultdict(set), defaultdict(float), defaultdict(list)
                if direction == 'out':
                    iter_ = df.itertuples(index=False, name=None)
                elif direction == 'in':
                    iter_ = ((v, u, t, f) for u, v, t, f in df.itertuples(index=False, name=None))
                elif direction == 'both':
                    iter_ = (it for u, v, t, f in df.itertuples(index=False, name=None) for it in [(u, v, t, f), (v, u, t, f)])
                else:
                    raise UnrecognizedDirection()
                for u, v, t, f in iter_:
                    if f:
                        # start
                        if t > e[u] and e[u] != 0:
                            out[u].append((e[u], len(active_neighbors[u])))
                        active_neighbors[u].add(v)
                    else:
                        # finish
                        if t > e[u]:
                            out[u].append((e[u], len(active_neighbors[u])))
                        active_neighbors[u].remove(v)
                    e[u] = t
                for u in out.keys():
                    if len(out[u]) or e[u] != out[u][-1][1] or out[u][-1][1] != len(active_neighbors[u]):
                        out[u].append((e[u], len(active_neighbors[u])))
                    out[u] = TimeCollection(out[u])
                return NodeCollection(out)
            else:
                return LinkSetDF(self.df.df_at(t)).degree(u=None, direction=direction)
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
                return TemporalNodeSetDF(df[['u', 'ts', 'tf']], disjoint_intervals=di).n_at(t=None)
            else:
                return len(set(df.df_at(t).u.values.flat))

    def neighbors(self, u=None, direction='out'):
        if not bool(self):
            if u is None:
                return dict()
            else:
                return TemporalNodeSetDF()
        if u is None:
            neighbors = defaultdict(list)
            if direction == 'out':
                def add(u, v, ts, tf):
                    neighbors[u].append((v, ts, tf))
            elif direction == 'in':
                def add(u, v, ts, tf):
                    neighbors[v].append((u, ts, tf))
            elif direction=='both':
                def add(u, v, ts, tf):
                    neighbors[u].append((v, ts, tf))
                    neighbors[v].append((u, ts, tf))
            else:
                raise UnrecognizedDirection()
            for u, v, ts, tf in self.dfm.itertuples():
                add(u, v, ts, tf)
            return NodeCollection({u: TemporalNodeSetDF(ns, disjoint_intervals=False) for u, ns in iteritems(neighbors)})
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
            if self.weighted:
                df = df.drop(columns = 'w')
            return TemporalNodeSetDF(df, disjoint_intervals=False)

    def degree_of(self, u=None, direction='out'):
        if not bool(self):
            if u is None:
                return dict()
            else:
                return TemporalNodeSetDF()
        if u is None:
            degree = Counter()
            dc = (1 if self.discrete else 0)
            if direction == 'out':
                def add(u, v, ts, tf):
                    degree[u] += tf - ts + dc
            elif direction == 'in':
                def add(u, v, ts, tf):
                    degree[v] += tf - ts + dc
            elif direction == 'both':
                def add(u, v, ts, tf):
                    degree[u] += tf - ts + dc
                    degree[v] += tf - ts + dc
            else:
                raise UnrecognizedDirection()
            for u, v, ts, tf in self.dfm.itertuples():
                add(u, v, ts, tf)
            return NodeCollection(degree)
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

    def substream(self, nsu, nsv, ts):
        if not isinstance(nsu, ABC.NodeSet):
            raise UnrecognizedNodeSet('nsu')
        if not isinstance(nsv, ABC.NodeSet):
            raise UnrecognizedNodeSet('nsv')
        if not isinstance(ts, ABC.TimeSet):
            raise UnrecognizedTimeSet('ts')
        assert self.discrete == ts.discrete
        if bool(self) and bool(nsu) and bool(nsv) and bool(ts):
            return TemporalLinkSetDF(self.df[self.df.u.isin(nsu) & self.df.v.isin(nsv)].intersect(utils.ts_to_df(ts), by_key=False, on_column=['u', 'v']), discrete=self.discrete, weighted=self.weighted)
        else:
            return TemporalLinkSetDF()

    def __and__(self, ls):
        if isinstance(ls, ABC.TemporalLinkSet):
            if bool(ls) and bool(self):
                assert ls.discrete == self.discrete
                if not isinstance(ls, TemporalLinkSetDF):
                    try:
                        return ls & self
                    except NotImplementedError:
                        ls = TemporalLinkSetDF(ls, discrete=self.discrete, weighted=self.weighted)
                return TemporalLinkSetDF(self.df.intersect(ls.df), discrete=self.discrete, weighted=self.weighted)
        else:
            raise UnrecognizedTemporalLinkSet('right operand')
        return TemporalLinkSetDF()

    def __or__(self, ls):
        if isinstance(ls, ABC.TemporalLinkSet):
            if not bool(self):
                return ls.copy()
            elif bool(ls):
                assert ls.discrete == self.discrete
                if not isinstance(ls, TemporalLinkSetDF):
                    try:
                        return ls | self
                    except NotImplementedError:
                        ls = TemporalLinkSetDF(ls, discrete=self.discrete, weighted=self.weighted)
                return TemporalLinkSetDF(self.df.union(ls.df), discrete=self.discrete, weighted=self.weighted)
            else:
                return self.copy()
        else:
            raise UnrecognizedTemporalLinkSet('right operand')

    def __sub__(self, ls):
        if isinstance(ls, ABC.TemporalLinkSet):
            if bool(self) and bool(ls):
                assert ls.discrete == self.discrete
                if isinstance(ls, LinkSetDF):
                    try:
                        return ls.__rsub__(self)
                    except (AttributeError, NotImplementedError):
                        ls = LinkSetDF(ls, discrete=self.discrete, weighted=self.weighted)
                return TemporalLinkSetDF(self.df.difference(ls.df), discrete=self.discrete, weighted=self.weighted)
        else:
            raise UnrecognizedTemporalLinkSet('right operand')
        return self.copy()

    def issuperset(self, ls):
        if isinstance(ls, ABC.TemporalLinkSet):
            if bool(self) and bool(ls):
                assert ls.discrete == self.discrete
                if not isinstance(ls, TemporalLinkSetDF):
                    try:
                        return ls.__issubset__(self)
                    except (AttributeError, NotImplementedError):
                        ls  = TemporalLinkSetDF(ls, discrete=self.discrete)
                return self.df.issuper(ls.df)
            else:
                return not bool(ls)
        else:
            raise UnrecognizedTemporalLinkSet('ls')
        return TemporalLinkSetDF()

    def _m_at_unweighted(self, t):
        if t is None:
            df = self.dfm.events[['u', 'v', 't', 'start']]
            u, v, e, f = df.iloc[0]
            active_links = {(u, v)}
            links = list()
            for u, v, t, f in df.iloc[1:].itertuples(index=False, name=None):
                if t > e:
                    links.append((e, len(active_links)))
                if f:
                    # start
                    active_links.add((u, v))
                else:
                    # finish
                    active_links.remove((u, v))
                e = t
            ef, nal = links[-1]
            if e != ef or nal != len(active_links):
                links.append((e, len(active_links)))
            return TimeCollection(links)
        else:
            return self.dfm.count_at(t)

    def _m_at_weighted(self, t):
        if t is None:
            df = self.dfm.events[['u', 'v', 't', 'start', 'w']]
            u, v, e, f, w = df.iloc[0]
            active_links, links = Counter(), list()
            active_links[(u, v)] += w
            for u, v, t, f, w in df.iloc[1:].itertuples(index=False, name=None):
                if t > e:
                    links.append((e, sum(itervalues(active_links))))
                active_links[(u, v)] += (w if f else -w)
                e = t
            ef, nal = links[-1]
            if e != ef or nal != sum(itervalues(active_links)):
                links.append((e, sum(itervalues(active_links))))
            return TimeCollection(links)
        else:
            return self.dfm.count_at(t, weights=True)

    def neighborhood(self, ns, direction='out'):
        # if df join on u / combine (intersect) and the union intervals (for union)
        # if range
        derror = False
        if not isinstance(ns, ABC.TemporalNodeSet):
            raise UnrecognizedTemporalNodeSet('ns')
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
                df = df.intersect(utils.ts_to_df(ns.timeset_), on_columns=['u', 'v'], by_key=False)
        else:
            base_df = utils.ns_to_df(ns)
            if direction == 'out':
                df = self.df.map_intersect(base_df)
            elif direction == 'in':
                df = self.df_.rename(columns={'u': 'v', 'v': 'u'}).map_intersect(base_df)
            elif direction == 'both':
                df = self.df.map_intersect(base_df)
                df = df.append(self.df_.rename(columns={'u': 'v', 'v': 'u'}).map_intersect(base_df), ignore_index=True)
            else:
                derror = True
        if derror:
            raise UnrecognizedDirection()
        return TemporalNodeSetDF(df, disjoint_intervals=False, discrete=self.discrete)

    def induced_substream(self, ns):
        if isinstance(ns, ABC.TemporalNodeSet):
            if bool(self) and bool(ns):
                assert ns.discrete == self.discrete
                if isinstance(ns, TemporalNodeSetB):
                    tdf = self.df_[self.df_['v'].isin(ns.nodeset_) & self.df_['u'].isin(ns.nodeset_)]
                    tdf = tdf.intersect(utils.ts_to_df(ns.timeset_), on_columns=['u', 'v'], by_key=False)
                    if not tdf.empty:
                        return TemporalLinkSetDF(tdf, discrete=self.discrete, weighted=self.weighted)
                else:
                    base_df = utils.ns_to_df(ns)
                    return TemporalLinkSetDF(self.df_.cartesian_intersect(base_df), discrete=self.discrete, weighted=self.weighted)
        else:
            raise UnrecognizedTemporalNodeSet('ns')
        return TemporalLinkSetDF()

    @property
    def m(self):
        if bool(self):
            return self.df_[['u', 'v']].drop_duplicates().shape[0]
        else:
            return 0

    def get_maximal_cliques(self, direction='both'):
        if not bool(self):
            return set()

        S, S_set, R, times, nodes = deque(), set(), set(), dict(), dict()

        if self.sort_by in [['ts'], ['ts', 'tf'], ['tf']]:
            df = self.df
        else:
            df = self.dfm.sort_values(by=['ts', 'tf'])

        def add_clique(c):
            if c[0:2] not in S_set:
                S.appendleft(c)
                S_set.add(c[0:2])

        if direction == 'out':
            def as_link(u, v):
                return (u, v)
            def add_nodes(u, v):
                nodes[u].add(v)
            def add_element(times, l, ts, tf):
                times[l].append((ts, tf))
                return True
        elif direction == 'in':
            def as_link(u, v):
                return (v, u)
            def add_nodes(u, v):
                nodes[v].add(u)
            def add_element(times, l, ts, tf):
                times[l].append((ts, tf))
                return True
        elif direction == 'both':
            def as_link(u, v):
                return frozenset([u, v])
            def add_nodes(u, v):
                nodes[v].add(u)
                nodes[u].add(v)
            def add_element(times, l, ts, tf):
                if len(times[l]):
                    tsp, tfp = times[l][-1]
                    assert ts >= tsp
                    if ts <= tfp:
                        times[l][-1] = (tsp, max(tf, tfp))
                        return False
                times[l].append((ts, tf))
                return True
        else:
            raise UnrecognizedDirection()

        times, nodes = defaultdict(list), defaultdict(set)
        for u, v, ts, tf, in df[['u', 'v', 'ts', 'tf']].itertuples(index=False, name=None):
            # This a new instance
            add_nodes(u, v)
            if add_element(times, as_link(u, v), ts, tf):
                add_clique((frozenset([u, v]), (ts, ts), set([])))

        def getTd(can, tb, te):
            td = 0
            min_t = None
            for u in can:
                for v in can:
                    link = as_link(u, v)
                    if link in times:
                        # Find min time x > te s.t. (b,x,u,v) exists in stream
                        tlist = times[link]
                        first, last = 0, len(tlist)-1
                        middle = int((first+last)/2.0)
                        
                        while first <= last:
                            if tlist[middle][0] > tb:
                                last = middle - 1
                            elif tlist[middle][1] < tb:
                                first = middle + 1
                            else:
                                # found a link that contains b
                                assert tlist[middle][0] <= tb and tlist[middle][1] >= tb
                                assert tlist[middle][1] >= te
                                if min_t != None:
                                    min_t = min(min_t, tlist[middle][1])
                                else:
                                    min_t = tlist[middle][1]
                                break
                            middle = (first + last ) // 2

                        # We should not be here except for a break in the while loop above
                        if not first <= last:
                            assert False
            return min_t

        def isClique(cnds, node, tb, te):
            """ returns True if X(c) union node is a clique over tb;te, False otherwise"""
            for i in cnds:
                if as_link(i, node) not in times:
                    # Check that (i, node) exists in stream.
                    return False
                else:
                    link = as_link(i, node)
                    # Check there is a link (b, e, i, node) s.t. b <= tb and e >= te, otherwise not a clique.
                    #tlist is a list of non-overlapping couples (b,e)
                    tlist = times[link]
                    # start binary search for b in tlist
                    first, last = 0, len(tlist)-1
                    middle = (first+last)//2
                    while first <= last:
                        if tlist[middle][0] > tb:
                            last = middle - 1
                        elif tlist[middle][1] < tb:
                            first = middle + 1
                        else:
                            # found a link that contains b
                            assert tlist[middle][0] <= tb and tlist[middle][1] >= tb
                            if tlist[middle][1] < te:
                                return False
                            break
                        middle = (first + last) // 2
                    # if we are here without having found a link that contains self._tb
                    if first > last:
                        return False
            return True

        while len(S) != 0:
            cnds, (ts, tf), can = S.pop()
            is_max = True

            # Grow time on the right side
            td = getTd(cnds, ts, tf)
            if td != tf:
                # nodes, (ts, tf), candidates
                add_clique((cnds, (ts, td), can))
                is_max = False

            # Grow node set
            can = set(can)
            if ts == tf:
                for u in cnds:
                    neighbors = nodes[u]
                    for n in neighbors:
                        can.add(n)    
            can -= cnds

            for node in can:
                if isClique(cnds, node, ts, tf):
                    # Is clique!
                    Xnew = set(cnds) | set([node])
                    add_clique((frozenset(Xnew), (ts, tf), can))
                    is_max = False

            if is_max:
                R.add((cnds, (ts, tf)))
        return R

    def _to_discrete(self, bins, bin_size):
        df, bins = time_discretizer_df(self.df, bins, bin_size, columns=['ts', 'tf'])
        return self.__class__(df, disjoint_intervals=False, discrete=True), bins
