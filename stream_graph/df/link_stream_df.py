import operator
from functools import reduce
from numbers import Real
from collections import deque
from collections import defaultdict

import pandas as pd

from . import utils
from stream_graph import ABC
from .node_stream_df import NodeStreamDF
from .node_stream_df import INodeStreamDF
from .link_set_df import LinkSetDF
from .time_set_df import TimeSetDF
from .time_df import IntervalDF
from .time_df import InstantaneousDF
from stream_graph.set.node_set_s import NodeSetS
from stream_graph.set.time_set_s import ITimeSetS
from stream_graph.node_stream_b import NodeStreamB
from stream_graph.exceptions import UnrecognizedLinkStream
from stream_graph.exceptions import UnrecognizedNodeStream
from stream_graph.exceptions import UnrecognizedNodeSet
from stream_graph.exceptions import UnrecognizedTimeSet
from stream_graph.exceptions import UnrecognizedDirection


class LinkStreamDF(ABC.LinkStream):
    def __init__(self, df=None, disjoint_intervals=True, sort_by=['u', 'v', 'ts', 'tf']):
        if df is not None:
            if isinstance(df, ABC.ILinkStream):
                if isinstance(df, ILinkStreamDF):
                    df = IntervalDF(df.df)
                else:
                    df = IntervalDF(list(iter(df)), columns=['u', 'v', 'ts'])
            elif not isinstance(df, IntervalDF):
                if not isinstance(df, pd.DataFrame):
                    df = IntervalDF(list(iter(df)), columns=['u', 'v', 'ts', 'tf'])
                else:
                    df = IntervalDF(df)
            self.df_ = df
            self.sort_by = sort_by
            self.merged_ = disjoint_intervals

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
            self.df_.union(inplace=True)
            self.merged_ = True
        return self

    @property
    def df(self):
        if bool(self):
            return self.merge_.sort_.df_
        else:
            return IntervalDF(columns=['u', 'v', 'ts', 'tf'])

    @property
    def dfm(self):
        if bool(self):
            return self.merge_.df_
        else:
            return IntervalDF(columns=['u', 'v', 'ts', 'tf'])

    @property
    def size(self):
        if bool(self):
            return self.dfm.measure_time()
        else:
            return 0

    @property
    def basic_nodestream(self):
        # Create node stream
        if bool(self):
            return NodeStreamB(self.nodeset, TimeSetDF([(self.df_.ts.min(), self.df_.tf.max())]))
        else:
            return NodeStreamB()

    @property
    def minimal_nodestream(self):
        # All the time intervals that a node belongs to a link
        if bool(self):
            mdf = self.df_[['v', 'ts', 'tf']].rename(columns={'v': 'u'}).append(self.df_[['u', 'ts', 'tf']])
            return NodeStreamDF(mdf, disjoint_intervals=False)
        else:
            return NodeStreamDF()

    @property
    def linkset(self):
        if bool(self):
            return LinkSetDF(self.df_[['u', 'v']].drop_duplicates())
        else:
            return LinkSetDF()

    @property
    def timeset(self):
        if bool(self):
            return TimeSetDF(self.df_[['ts', 'tf']], disjoint_intervals=False)
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
            return self.df.itertuples(index=False, name=None)
        else:
            return iter([])

    def links_at(self, t):
        if not bool(self):
            return LinkSetDF()
        if isinstance(t, tuple) and len(t) == 2 and isinstance(t[0], Real) and isinstance(t[1], Real) and t[0]<=t[1]:
            return LinkSetDF(self.df.df_at_interval(t[0], t[1]).drop(columns=['ts', 'tf']), no_duplicates=False)
        else:
            return LinkSetDF(self.df.df_at(t).drop(columns=['ts', 'tf']), no_duplicates=False)

    def times_of(self, l=None, direction='out'):
        if not bool(self):
            if l is None:
                return {}
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
            for u, v, ts, tf in iter(self):
                add((u, v))
            return {l: TimeSetDF(ts, disjoint_intervals=di) for l, ts in iteritems(times)}
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
            return TimeSetDF(df.drop(columns=['u', 'v']), disjoint_intervals=di)

    def neighbors_at(self, u, t, direction='out'):
        if not bool(self):
            return NodeSetS()
        if direction == 'out':
            df = self.df[self.df.u == u].drop(columns=['u']).rename(columns={'v': 'u'})
        elif direction == 'in':
            df = self.df[self.df.v == u].drop(columns=['v'])
        elif direction == 'both':
            df = self.df[self.df.u == u].drop(columns=['u']).rename(columns={'v': 'u'})
            df = df.append(self.df[self.df.v == u].drop(columns=['v']), ignore_index=True)
        else:
            raise UnrecognizedDirection()
        return NodeSetS(df[df.index_at(t)].u.values.flat)

    def neighbors(self, u=None, direction='out'):
        if not bool(self):
            if u is None:
                return dict()
            else:
                return NodeStreamDF()
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
            for u, v, ts, tf in iter(self):
                add(u, v, ts, tf)
            return {u: NodeStreamDF(ns, disjoint_intervals=False) for u, ns in iteritems(neighbors)}
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
            return NodeStreamDF(df, disjoint_intervals=False)

    def substream(self, nsu, nsv, ts):
        if not isinstance(nsu, ABC.NodeSet):
            raise UnrecognizedNodeSet('nsu')
        if not isinstance(nsv, ABC.NodeSet):
            raise UnrecognizedNodeSet('nsv')
        if not isinstance(ts, ABC.TimeSet):
            raise UnrecognizedTimeSet('ts')
        if bool(self) and bool(nsu) and bool(nsv) and bool(ts):
            return LinkStreamDF(self.df[self.df.u.isin(nsu) & self.df.v.isin(nsv)].intersect(utils.ts_to_df(ts), by_key=False, on_column=['u', 'v']))
        else:
            return LinkStreamDF()

    def __and__(self, ls):
        if isinstance(ls, ABC.LinkStream):
            if bool(ls) and bool(self):
                if not isinstance(ls, LinkStreamDF):
                    try:
                        return ls & self
                    except NotImplementedError:
                        ls = LinkStreamDF(ls)
                return LinkStreamDF(self.df.intersect(ls.df))
        else:
            raise UnrecognizedLinkStream('right operand')
        return LinkStreamDF()

    def __or__(self, ls):
        if isinstance(ls, ABC.LinkStream):
            if not bool(self):
                return ls.copy()
            elif bool(ls):
                if not isinstance(ls, LinkStreamDF):
                    try:
                        return ls | self
                    except NotImplementedError:
                        ls = LinkStreamDF(ls)
                return LinkStreamDF(self.df.union(ls.df))
            else:
                return self.copy()
        else:
            raise UnrecognizedLinkStream('right operand')

    def __sub__(self, ls):
        if isinstance(ls, ABC.LinkStream):
            if bool(self) and bool(ls):
                if isinstance(ls, LinkSetDF):
                    try:
                        return ls.__rsub__(self)
                    except (AttributeError, NotImplementedError):
                        ls = LinkSetDF(ls)
                return LinkStreamDF(self.df.difference(ls.df))
        else:
            raise UnrecognizedLinkStream('right operand')
        return self.copy()

    def issuperset(self, ls):
        if isinstance(ls, ABC.LinkStream):
            if bool(self) and bool(ls):
                if not isinstance(ls, LinkStreamDF):
                    try:
                        return ls.__issubset__(self)
                    except (AttributeError, NotImplementedError):
                        ls  = LinkStreamDF(ls)
                return self.df.issuper(ls.df)
            else:
                return not bool(ls)
        else:
            raise UnrecognizedLinkStream('ls')
        return LinkStreamDF()

    def number_of_links_at(self, t):
        if bool(self):
            return self.dfm.df_count_at(t)
        return 0

    def linkstream_at(self, t):
        if bool(self):
            return LinkStreamDF(self.dfm.df_at(t), sort_by=self.sort_by)
        return LinkStreamDF()

    def neighborhood(self, ns, direction='out'):
        # if df join on u / combine (intersect) and the union intervals (for union)
        # if range
        derror = False
        if not isinstance(ns, ABC.NodeStream):
            raise UnrecognizedNodeStream('ns')
        if isinstance(ns, NodeStreamB):
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
        return NodeStreamDF(df, disjoint_intervals=False)

    def induced_substream(self, ns):
        if isinstance(ns, ABC.NodeStream):
            if bool(self) and bool(ns):
                if isinstance(ns, NodeStreamB):
                    tdf = self.df_[self.df_['v'].isin(ns.nodeset_) & self.df_['u'].isin(ns.nodeset_)]
                    tdf = tdf.intersect(utils.ts_to_df(ns.timeset_), on_columns=['u', 'v'], by_key=False)
                    if not tdf.empty:
                        return LinkStreamDF(tdf)
                else:
                    base_df = utils.ns_to_df(ns)
                    return LinkStreamDF(self.df_.cartesian_intersect(base_df))
        else:
            raise UnrecognizedNodeStream('ns')
        return LinkStreamDF()

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

        if self.sort_by == ['ts', 'tf']:
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

        times, nodes = defaultdict(list), defaultdict(set)
        for u, v, ts, tf, in df[['u', 'v', 'ts', 'tf']].itertuples(index=False, name=None):
            # This a new instance
            times[as_link(u, v)].append((ts, tf))
            add_nodes(u, v)
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
                        assert first <= last
                                
            return min_t

        def isClique(cnds, node, te, tb):
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
                        middle = (first + last ) // 2
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

class ILinkStreamDF(ABC.ILinkStream, LinkStreamDF):
    def __init__(self, df=None, disjoint_intervals=True, sort_by=['u', 'v', 'ts']):
        if df is not None:
            if not isinstance(df, InstantaneousDF):
                if not isinstance(df, pd.DataFrame):
                    df = InstantaneousDF(list(iter(df)), columns=['u', 'v', 'ts'])
                else:
                    df = InstantaneousDF(df)
            self.df_ = df
            self.sort_by = sort_by
            self.merged_ = disjoint_intervals

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
    def basic_nodestream(self):
        # Create node stream
        if bool(self):
            return NodeStreamB(self.nodeset, ITimeSetDF([(self.df_.ts.min(), self.df_.ts.max())]))
        else:
            return NodeStreamB()

    @property
    def minimal_nodestream(self):
        # All the time intervals that a node belongs to a link
        if bool(self):
            mdf = self.df_[['v', 'ts']].rename(columns={'v': 'u'}).append(self.df_[['u', 'ts']])
            return INodeStreamDF(mdf, disjoint_intervals=False)
        else:
            return INodeStreamDF()


    @property
    def timeset(self):
        if bool(self):
            return ITimeSetS(self.df_[['ts']].values.flat)
        else:
            return ITimeSetS()

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

    def __iter__(self):
        if bool(self):
            return self.df.itertuples(index=False, name=None)
        else:
            return iter([])

    def links_at(self, t):
        if not bool(self):
            return LinkSetDF()
        else:
            return LinkSetDF(self.df.df_at(t).drop(columns=['ts']), no_duplicates=False)

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
            for u, v, ts, tf in iter(self):
                times[key(u, v)].add(ts)
            return {l: TimeSetS(ts) for l, ts in iteritems(times)}
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
            return ITimeSetS(df['ts'].values.flat)

    def neighbors(self, u=None, direction='out'):
        if not bool(self):
            if u is None:
                return {}
            else:
                return NodeStreamDF()

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
            for u, v, ts in iter(self):
                add(u, v, ts)
            return {u: INodeStreamDF(ns) for u, ns in iteritems(neighbors)}
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
            return INodeStreamDF(df, disjoint_intervals=False)

    def substream(self, nsu, nsv, ts):
        if not isinstance(nsu, ABC.NodeSet):
            raise UnrecognizedNodeSet('nsu')
        if not isinstance(nsv, ABC.NodeSet):
            raise UnrecognizedNodeSet('nsv')
        if not isinstance(ts, ABC.TimeSet):
            raise UnrecognizedTimeSet('ts')
        if bool(self) and bool(nsu) and bool(nsv) and bool(ts):
            if isinstance(ts, ABC.ITimeSet):
                return ILinkStreamDF(self.df[self.df.u.isin(nsu) & self.df.v.isin(nsv)].intersect(utils.its_to_idf(ts), by_key=False, on_column=['u', 'v']))
            else:
                dfp = self.df[self.df.u.isin(nsu) & self.df.v.isin(nsv)]
                dfp['tf'] = dfp['ts']
                return ILinkStreamDF(pd.IntervalDF(dfp.intersect(InstantaneousDF(list(ts), columns=["ts"]), by_key=False, on_column=['u', 'v'])[['u', 'v', 'ts']]))
        else:
            return LinkStreamDF()

    def __and__(self, ls):
        if isinstance(ls, ABC.LinkStream):
            if bool(ls) and bool(self):
                if isinstance(ls, ABC.ILinkStream):
                    if not isinstance(ls, ILinkStreamDF):
                        try:
                            return ls & self
                        except NotImplementedError:
                            ls = ILinkStreamDF(ls)
                    return ILinkStreamDF(self.df.intersect(ls.df))
                else:
                    return ILinkStreamDF(LinkStreamDF(LinkStreamDF(self) & ls).df.drop(columns=['tf']))
        else:
            raise UnrecognizedLinkStream('right operand')
        return LinkStreamDF()

    def __or__(self, ls):
        if isinstance(ls, ABC.LinkStream):
            if not bool(self):
                return ls.copy()
            elif bool(ls):
                if isinstance(ls, ABC.ILinkStream):
                    if not isinstance(ls, ILinkStreamDF):
                        try:
                            return ls | self
                        except NotImplementedError:
                            ls = ILinkStreamDF(ls)
                    return ILinkStreamDF(self.df.union(ls.df))
                else:
                    return LinkStreamDF(self) | ls
            else:
                return self.copy()
        else:
            raise UnrecognizedLinkStream('right operand')

    def __sub__(self, ls):
        if isinstance(ls, ABC.LinkStream):
            if bool(self) and bool(ls):
                if isinstance(ls, ABC.ILinkStream):
                    if not isinstance(ls, ILinkStreamDF):
                        try:
                            return ls.__rsub__(self)
                        except (AttributeError, NotImplementedError):
                            ls = ILinkStreamDF(ls)
                    return ILinkStreamDF(self.df.difference(ls.df))
                else:
                    return LinkStreamDF(self) - ls
        else:
            raise UnrecognizedLinkStream('right operand')
        return self.copy()

    def issuperset(self, ls):
        if isinstance(ls, ABC.LinkStream):
            if bool(self) and bool(ls):
                if isinstance(ls, ABC.ILinkStream):
                    if not isinstance(ls, ILinkStreamDF):
                        try:
                            return ls.__issubset__(self)
                        except (AttributeError, NotImplementedError):
                            ls = ILinkStreamDF(ls)
                    return self.df.issuper(ls.df)
                else:
                    return LinkStreamDF(self).issuper(ls)
            else:
                return not bool(ls)
        else:
            raise UnrecognizedLinkStream('ls')
        return False

    def neighborhood(self, ns, direction='out'):
        # if df join on u / combine (intersect) and the union intervals (for union)
        # if range
        derror = False
        if not isinstance(ns, ABC.NodeStream):
            raise UnrecognizedNodeStream('ns')
        if isinstance(ns, NodeStreamB):
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
            if isinstance(ns, ABC.INodeStream):
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
        return INodeStreamDF(df, disjoint_intervals=False)

    def induced_substream(self, ns):
        if isinstance(ns, ABC.NodeStream):
            if bool(self) and bool(ns):
                if isinstance(ns, NodeStreamB):
                    tdf, ts = self.df_[self.df_['v'].isin(ns.nodeset_) & self.df_['u'].isin(ns.nodeset_)], ns.timeset_
                    if isinstance(ts, ABC.ITimeSet):
                        ts = utils.its_to_idf(ts)
                    else:
                        df, ts = IntervalDF(self.df), utils.ts_to_df(ts)
                    tdf = tdf.intersect(ts, on_columns=['u', 'v'], by_key=False)
                else:
                    base_df = utils.ns_to_df(ns)
                    if isinstance(ns, ABC.INodeStream):
                        df, base_df = self.dfm, utils.ins_to_idf(ns)
                    else:
                        df, base_df = IntervalDF(self.dfm), utils.ns_to_df(ns)
                    tdf = self.df_.cartesian_intersect(base_df)
                if not tdf.empty:
                    if isinstance(tdf, IntervalDF):
                        tdf = tdf.drop(columns=['tf'])   
                return ILinkStreamDF(tdf)
        else:
            raise UnrecognizedNodeStream('ns')
        return ILinkStreamDF()

    def get_maximal_cliques(self, delta, direction='both'):
        df = self.df.copy()
        df['ts'] += delta*0.5
        df['tf'] = df['ts'] + delta
        return LinkStreamDF(df, disjoint_intervals=(delta == .0)).get_maximal_cliques(direction=direction)


    def centrality(self, u=None, direction='both'):
        if u is None:
            return {u: self.centrality(u, direction=direction) for u in self.nodeset}
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

        lines = list((u, v, ts) for u, v, ts, in df.sort_values(by=['ts']).itertuples(index=False, name=None))            
        return ego(u, self.linkset.neighbors(u, direction=direction).nodes_, lines, both)
