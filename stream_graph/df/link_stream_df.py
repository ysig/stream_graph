import operator
from functools import reduce
from numbers import Real
from collections import deque
from collections import defaultdict

import pandas as pd

from . import utils
from stream_graph import ABC
from .node_stream_df import NodeStreamDF
from .link_set_df import LinkSetDF
from .time_set_df import TimeSetDF
from .interval_df import IntervalDF
from stream_graph.set.node_set_s import NodeSetS
from stream_graph.node_stream_b import NodeStreamB
from stream_graph.exceptions import UnrecognizedLinkStream
from stream_graph.exceptions import UnrecognizedNodeStream
from stream_graph.exceptions import UnrecognizedNodeSet
from stream_graph.exceptions import UnrecognizedTimeSet
from stream_graph.exceptions import UnrecognizedDirection


class LinkStreamDF(ABC.LinkStream):
    def __init__(self, df=None, disjoint_intervals=True, sort_by=['u', 'v', 'ts', 'tf']):
        if df is not None:
            if not isinstance(df, IntervalDF):
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

    def times_of(self, u, v, direction='out'):
        if not bool(self):
            return TimeSetDF()
        di = True
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

    def neighbors(self, u, direction='out'):
        if not bool(self):
            return NodeStreamDF()
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
                    except NotImplementedError or NotImplemented:
                        df = LinkStreamDF(ls)
                    return self.df.issuper(ls.df)
                else:
                    return self.copy()
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
                df = utils.intersect_intervals_with_df(df, utils.ts_to_df(ns.timeset_), on_columns=['u', 'v'])
        else:
            base_df = utils.ns_to_df(ns)
            if direction == 'out':
                df = self.df.map_intersect(base_df)
            elif direction == 'in':
                df = self.df_.rename(columns={'u': 'v', 'v': 'u'}).map_intersect_df(base_df)
            elif direction == 'both':
                df = self.df.map_intersect(base_df)
                df = df.append(self.df_.rename(columns={'u': 'v', 'v': 'u'}).map_intersect_df(base_df), ignore_index=True)
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
                    tdf = tdf.intersect_intervals(utils.ts_to_df(ns.timeset_), on_columns=['u', 'v'], by_key=False)
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
        for u, v, ts, tf, in df.itertuples(index=False, name=None):
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
                        middle = (first+last)//2
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
