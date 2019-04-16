import operator
from functools import reduce
from numbers import Real
from collections import defaultdict
from collections import Counter
from warnings import warn
from six import iteritems

import pandas as pd

from . import utils
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
    def __init__(self, df=None, no_duplicates=True, sort_by=['u', 'v', 'ts'], discrete=None, weighted=False):
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
                    df.columns = ['u', 'v', 'ts']
                    if weighted:
                        if not no_duplicates:
                            df, no_duplicates = InstantaneousDF(df).merge(), True
                elif len(df.columns) == 4 :
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
            return LinkSetDF(self.df_[['u', 'v']].drop_duplicates(), weighted=self.weighted)
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
            return TemporalNodeSetB(self.nodeset, ITimeSetDF([(self.df_.ts.min(), self.df_.ts.max())], discrete=self.discrete))
        else:
            return TemporalNodeSetB()

    @property
    def minimal_temporal_nodeset(self):
        # All the time intervals that a node belongs to a link
        if bool(self):
            mdf = self.df_[['v', 'ts']].rename(columns={'v': 'u'}).append(self.df_[['u', 'ts']])
            return ITemporalNodeSetDF(mdf, disjoint_intervals=False, discrete=self.discrete)
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
                def generate(df):
                    for _, df_grouped in df:
                        ts = df_grouped.ts.iloc[0]
                        yield (ts, LinkSetDF(df_grouped.drop(columns='ts'), no_duplicates=False, weighted=self.weighted).merge_)
                return TimeGenerator(generate(self.dfm.groupby('ts')), True)
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
                for _, df_grouped in self.dfm.groupby('ts'):
                    ts = df_grouped.ts.iloc[0]
                    cache = defaultdict(set)
                    for u, v in df_grouped[['u', 'v']].itertuples(index=False, name=None):
                        add(cache, u, v)
                    for u, s in iteritems(cache):
                        out[u].append((ts, NodeSetS(s)))
                for u in out.keys():
                    out[u] = TimeCollection(out[u])
                return NodeCollection(out)
            else:
                return LinkSetDF(self.dfm.df_at(t), weighted=self.weighted).neighbors(u=None, direction=direction)
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
                
                for _, df_grouped in self.dfm.groupby('ts'):
                    ts = df_grouped.ts.iloc[0]
                    cache = Counter()
                    for u, v, w in df_grouped[['u', 'v', 'w']].itertuples(index=False, name=None):
                        add(cache, u, v, w)
                    for u, s in iteritems(cache):
                        out[u].append((ts, w))
                for u in out.keys():
                    out[u] = TimeCollection(out[u])
                return NodeCollection(out)
            else:
                return LinkSetDF(self.dfm.df_at(t), weighted=self.weighted).degree(u=None, direction=direction)
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
                def generate(df):
                    for _, df_grouped in df:
                        ts = df_grouped.iloc[0]['ts']
                        yield (ts, df_grouped.w.sum())
                return TimeCollection(generate(df.groupby('ts')), True)
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

    def substream(self, nsu, nsv, ts):
        if not isinstance(nsu, ABC.NodeSet):
            raise UnrecognizedNodeSet('nsu')
        if not isinstance(nsv, ABC.NodeSet):
            raise UnrecognizedNodeSet('nsv')
        if not isinstance(ts, ABC.ITimeSet):
            raise UnrecognizedTimeSet('ts')
        if bool(self) and bool(nsu) and bool(nsv) and bool(ts):
            df, ts = self.df[self.df.u.isin(nsu) & self.df.v.isin(nsv)], set(t for t in ts)
            df = df.intersect(ts, by_key=False, on_column=['u', 'v'])
            return self.__class__(df, discrete=self.discrete, weighted=self.weighted)
        else:
            return TemporalLinkSetDF()

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
        df['ts'] += delta*0.5
        df['tf'] = df['ts'] + delta
        return TemporalLinkSetDF(df, disjoint_intervals=(delta == .0), discrete=self.discrete, weighted=self.weighted).get_maximal_cliques(direction=direction)


    def centrality(self, u=None, direction='both'):
        def ego(e, ne, l, both):
            # print >> sys.stderr, "Running node : " + str(e)
            u, v, t, index, times = 0, 0, 0, 0, list()

            ce, info = dict(), dict()
            for i in ne:
                info[(e,i)] = -1
                for x in ne-{i}:
                    info[(i,x)] = -1
                info[(i,e)] = -1

            index = 0
            time = l[index][2] #starting time.
            ne_x, lines, paths = ne | {e}, dict(), dict()

            if both:
                def add_lines(u, v, t):
                    lines[(u,v)] = t
                    lines[(v,u)] = t 
            else:
                def add_lines(u, v, t):
                    lines[(u,v)] = t

            while(index < len(l) -1):
                # get all links of time stamp
                while(index < len(l) -1):
                    u,v,t = l[index]
                    add_lines(u, v, t)
                    index += 1
                    if(t != time):
                        break

                for u in ne:
                    for v in ne-{u}:
                        # print u,v
                        ce[(u,v)] = 0.0
                        Q = set()
                        if (u,v) not in lines:
                            # print u,v
                            news = info[(u,v)]
                            for x in ne_x - {u, v}:
                                ux = info[(u, x)]
                                if (x, v) in lines:
                                    xv = lines[(x, v)]
                                else:
                                    xv = info[(x, v)]
                                if ux != -1 and xv != -1 and ux < xv: 
                                    if ux == news:
                                        Q.add(x)
                                    if ux > news:
                                        Q = {x}
                                        news = ux

                            if (u,v) in paths:
                                old_paths = paths[(u,v)]
                                if old_paths[0] == news:
                                    paths[(u, v)] = (news, paths[(u,v)][1] | Q)
                                elif old_paths[0] < news:
                                    paths[(u, v)] = (news, Q)
                            else:
                                paths[(u, v)] = (news, Q)

                            if e in paths[(u, v)][1]:
                                ce[(u, v)] = 1.0/len(paths[(u, v)][1])
                        else:
                            paths[(u, v)] = (t, {u})
                        
        
                times.append((time, sum(ce.values())))
                for k in lines:
                    info[k] = lines[k]
                time, lines = l[index][2], {}
            return times

            def as_link(u, v):
                return (u, v)
            def add_nodes(u, v):
                nodes[u].add(v)
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

        lines = list(key for key in df[['u', 'v', 'ts']].itertuples(index=False, name=None))            
        if u is None:
            return NodeCollection({u: ego(u, self.linkset.neighbors(u, direction=direction).nodes_, lines, both) for u in self.nodeset})
        else:
            return ego(u, self.linkset.neighbors(u, direction=direction).nodes_, lines, both)

    def _to_discrete(self, bins, bin_size):
        df, bins = time_discretizer_df(self.df, bins, bin_size, columns=['ts'])
        return self.__class__(df, no_duplicates=False, discrete=True), bins
