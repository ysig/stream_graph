import pandas as pd

from stream_graph import API
from stream_graph.df import utils
from stream_graph.df.node_stream_df import NodeStreamDF
from stream_graph.df.link_set_df import LinkSetDF
from stream_graph.df.time_set_df import TimeSetDF
from stream_graph.range.node_stream_r import NodeStreamR
from stream_graph.set.node_set_s import NodeSetS
from stream_graph.exceptions import UnrecognizedLinkStream
from stream_graph.exceptions import UnrecognizedNodeStream
from stream_graph.exceptions import UnrecognizedNodeSet
from stream_graph.exceptions import UnrecognizedTimeSet
from stream_graph.exceptions import UnrecognizedDirection


class LinkStreamDF(API.LinkStream):
    def __init__(self, df=None, disjoint_intervals=True, sort_by=['u', 'v', 'ts', 'tf']):
        if df is not None:
            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame(iter(df), columns=['u', 'v', 'ts', 'tf'])
            self.df_ = df
            self.sort_by = sort_by
            self.merged_ = not disjoint_intervals

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
            self.df_ = self.df_.sort_values(by=self.sort_by)
        return self

    @property
    def merge_(self):
        if not self.is_merged_:
            self.df_ = utils.merge_intervals_df(self.df_)
            self.is_merged_ = True
        return self

    @property
    def df(self):
        return self.merge_.sort_.df_

    @property
    def dfm(self):
        return self.merge_.df_

    @property
    def size(self):
        return utils.measure_time(self.df)

    @property
    def basic_nsm(self):
        nodes = self.df_.v.drop_duplicates().append(
            self.df_.u.drop_duplicates(),
            ignore_index=True).drop_duplicates().values
        # Create node stream
        return NodeStreamR(self.df_.ts.min(), self.df_.tf.max(), utils.nodes_to_range(set(nodes.flat), nodes.min(), nodes.max()))

    @property
    def minimal_nsm(self):
        # All the time intervals that a node belongs to a link
        mdf = self.df_[['v', 'ts', 'tf']].rename(columns={'v': 'u'}).append(self.df_[['u', 'ts', 'tf']])
        return NodeStreamDF(mdf, disjoint_intervals=False)

    @property
    def linkset(self):
        return LinkSetDF(self.df_[['u', 'v']].drop_duplicates())

    @property
    def timeset(self):
        return TimeSetDF(self.df_[['ts', 'tf']], disjoint_intervals=False)

    @property
    def nodeset(self):
        nodes = self.df.v.drop_duplicates().append(
            self.df.u.drop_duplicates(),
            ignore_index=True).drop_duplicates().values
        return NodeSetS(nodes.flat)

    def __contains__(self, v):
        assert isinstance(v, tuple) and len(v) == 3
        return ((self.df.u == v[0]) & (self.df.v == v[1]) & utils.df_index_at(self.df, v[2])).any()

    def links_at(self, t):
        if isinstance(t, TimeSetDF):
            return LinkSetDF(utils.common_time_with_df(self.df, TimeSetDF(t).df))
        else:
            return LinkSetDF(utils.df_at(self.df, t).drop(columns=['ts', 'tf']))

    def times_of(self, u, v, direction='out'):
        if direction == 'out':
            df = self.df[(self.df.u == u) & (self.df.v == v)]
        elif direction == 'in':
            df = self.df[(self.df.v == u) & (self.df.u == v)]
        elif direction == 'both':
            df = self.df[self.df.u.isin({u, v}) & self.df.v.isin({u, v})]
        else:
            raise UnrecognizedDirection()
        return TimeSetDF(df.drop(columns=['u', 'v']), disjoint_intervals=False)

    def neighbors_at(self, u, t, direction='out'):
        if direction == 'out':
            df = self.df[self.df.u == u].drop(columns=['u']).rename(columns={'u': 'v'})
        elif direction == 'in':
            df = self.df[self.df.v == u].drop(columns=['v'])
        elif direction == 'both':
            df = self.df[self.df.u == u].drop(columns=['u']).rename(columns={'u': 'v'})
            df = df.append(self.df[self.df.v == u].drop(columns=['v']), ignore_index=True)
        else:
            raise UnrecognizedDirection()
        return NodeSetS(self.df[utils.df_index_at(df, t)].drop(columns=['v', 'ts', 'tf']).values.flat)

    def neighbors(self, u, direction='out'):
        if direction == 'out':
            df = self.df[self.df.u == u].drop(columns=['u']).rename(columns={'u': 'v'})
        elif direction == 'in':
            df = self.df[self.df.v == u].drop(columns=['v'])
        elif direction=='both':
            df = self.df[self.df.u == u].drop(columns=['u']).rename(columns={'u': 'v'})
            df = df.append(self.df[self.df.v == u].drop(columns=['v']), ignore_index=True)
        else:
            raise UnrecognizedDirection()
        return utils.df_to_ns(df, merged=False)

    def substream(self, nsu, nsv, ts):
        if not isinstance(nsu, API.NodeSet):
            raise UnrecognizedNodeSet('nsu')
        if not isinstance(nsv, API.NodeSet):
            raise UnrecognizedNodeSet('nsv')
        if not isinstance(ts, API.TimeSet):
            raise UnrecognizedTimeSet('ts')
        if nsu and nsv and ts:
            return LinkStreamDF(utils.merge_intervals_with_df(
                self.df[self.df.u.isin(nsu) & self.df.v.isin(nsv)],
                utils.ts_to_df(ts), on_column=['u', 'v']))
        else:
            return LinkStreamDF()

    def __and__(self, ls):
        if isinstance(ls, API.LinkStream):
            if bool(ls) and bool(self):
                if not isinstance(ls, LinkStreamDF):
                    try:
                        return ls & self
                    except NotImplementedError:
                        ls = LinkStreamDF(ls)
                return LinkStreamDF(utils.intersect_intervals_df(self.df.append(ls.df, ignore_index=True),
                                                                 on_column=['u', 'v']))
        else:
            raise UnrecognizedLinkStream('right operand')
        return LinkStreamDF()

    def __or__(self, ls):
        if isinstance(ls, API.LinkStream):
            if not bool(self):
                return ls.copy()
            elif bool(ls):
                if not isinstance(ls, LinkStreamDF):
                    try:
                        return ls | self
                    except NotImplementedError:
                        ls = LinkStreamDF(ls)
                return LinkStreamDF(utils.merge_intervals_df(self.df.append(ls.df.append, ignore_index=True),
                                                             on_column=['u', 'v']))
            else:
                return self.copy()
        else:
            raise UnrecognizedLinkStream('right operand')

    def __sub__(self, ls):
        if isinstance(ls, API.LinkStream):
            if bool(self) and bool(ls):
                try:
                    return ls.__rsub__(self)
                except (AttributeError, NotImplementedError):
                    df = LinkSetDF(ls)
                return LinkStreamDF(utils.difference_df(self.df, ls.df, on_column=['u', 'v']))
        else:
            raise UnrecognizedLinkStream('right operand')
        return self.copy()

    def issuperset(self, ls):
        if isinstance(ls, API.LinkStream):
            if bool(self) and bool(ls):
                if not isinstance(ls, LinkStreamDF):
                    try:
                        return ls.__issubset__(self)
                    except NotImplementedError or NotImplemented:
                        df = LinkStreamDF(ls)
                    return utils.issuper_df(self.df, ls.df, on_column=['u', 'v'])
                else:
                    return self.copy()
            else:
                return not bool(ls)
        else:
            raise UnrecognizedLinkStream('ls')
        return LinkStreamDF()

    def number_of_links_at(self, t):
        return utils.df_count_at(self.dfm, t)

    def linkstream_at(self, t):
        return LinkStreamDF(utils.df_at(self.dfm, t), sort_by=self.sort_by)

    def neighborhood(self, ns, direction='out'):
        # if df join on u / combine (intersect) and the merge intervals (for union)
        # if range
        derror = False
        if not isinstance(ns, API.NodeStream):
            raise UnrecognizedNodeStream('ns')
        if isinstance(ns, NodeStreamR):
            if direction == 'out':
                df = self.df.rename(columns={'v': 'u', 'u': 'v'})
                df = df[df.v.isin(ns.nodes)].drop('v', axis=1)
            elif direction == 'in':
                df = self.df[self.df.v.isin(ns.nodes)].drop('v', axis=1)
            elif direction == 'both':
                df = self.df.rename(columns={'v': 'u', 'u': 'v'})
                df = df[df.v.isin(ns.nodes)].drop('v', axis=1)
                df = df.append(self.df[self.df.v.isin(ns.nodes)].drop('v', axis=1))
            else:
                derror = True
            if not derror:
                df = utils.df_fit_to_time_bounds(df, ns.min_time, ns.max_time)
            mnt, mxt, nodes = ns.min_time, ns.max_time, ns.nodes
        else:
            base_df = utils.ns_to_df(ns)
            if direction == 'out':
                df = utils.map_intersect_df(self.df, base_df)
            elif direction == 'in':
                df = utils.map_intersect_df(self.df.rename(columns={'u': 'v', 'v': 'u'}), base_df)
            elif direction == 'both':
                df = utils.map_intersect_df(self.df, base_df)
                df = df.append(utils.map_intersect_df(self.df_.rename(columns={'u': 'v', 'v': 'u'}), base_df), ignore_index=True)
            else:
                derror = True
            mnt, mxt, nodes = None, None, None
        if derror:
            raise UnrecognizedDirection()
        return utils.df_to_ns(df, mnt=mnt, mxt=mxt, nodes=nodes, merged=False)

    def induced_substream(self, ns):
        if isinstance(ns, API.NodeStream):
            if bool(ns):
                if isinstance(ns, NodeStreamR):
                    tdf = utils.df_fit_to_time_bounds(self.df_[self.df_['v'].isin(ns.nodes) & self.df_['u'].isin(ns.nodes)], ns.min_time, ns.max_time)
                    if not tdf.empty:
                        return LinkStreamDF(tdf)
                else:
                    base_df = utils.ns_to_df(ns)
                    return LinkStreamDF(utils.cartesian_intersect_df(self.df_, base_df))
        else:
            raise UnrecognizedNodeStream('ns')
        return LinkStreamDF()
