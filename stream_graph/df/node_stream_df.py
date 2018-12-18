import pandas as pd

from stream_graph import API
from stream_graph.df import utils
from stream_graph.df.time_set_df import TimeSetDF
from stream_graph.set.node_set_s import NodeSetS
from stream_graph.node_stream_b import NodeStreamB
from stream_graph.exceptions import UnrecognizedNodeStream


class NodeStreamDF(API.NodeStream):
    def __init__(self, df=None, disjoint_intervals=True, sort_by=['u', 'ts', 'tf']):
        if df is not None:
            if isinstance(df, pd.DataFrame):
                df = pd.DataFrame()
            self.df_ = df
            self.sort_by = sort_by
            self.merged_ = disjoint_intervals
            self.sorted_ = False

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
            self.df_ = self.df_.sort_values(by=self.sort_by)
        return self

    @property
    def merge_df_(self):
        if not self.merged_:
            self.df_ = utils.merge_intervals_df(self.df_)
            self.merged_ = True
        return self

    @property
    def df(self):
        return self.sort_df_.merge_df_.df_

    @property
    def dfm(self):
        return self.merge_df_.df

    @property
    def n(self):
        denom = float(self.total_time)
        if denom == .0:
            return .0
        else:
            return self.size/denom

    @property
    def timeset(self):
        return TimeSetDF(self.df_[['ts', 'tf']], disjoint_intervals=False)

    @property
    def nodeset(self):
        return NodeSetS(self.df_.u.drop_duplicates().values.flat)

    @property
    def total_common_time(self):
        # sum of cartesian interval intersection
        return utils.interval_intersection_size(self.df)

    @property
    def size(self):
        return utils.measure_time(self.df)

    def __iter__(self):
        return self.df.itertuples(index=False, name=None)

    def __bool__(self):
        return hasattr(self, 'df_') and self.df_.empty

    def __and__(self, ns):
        if isinstance(ns, API.NodeStream):
            if ns and bool(self):
                if isinstance(ns, NodeStreamB):
                    return NodeStreamDF(utils.intersect_intervals_with_df(
                        self.df[self.df.u.isin(ns.nodestream)],
                        TimeSetDF(ns.timeset).df))
                else:
                    if not isinstance(ns, NodeStreamDF):
                        try:
                            return ns & self
                        except NotImplementedError:
                            pass
                    df = utils.intersect_intervals_df(
                        utils.ns_to_df(ns).append(self.df,
                                                  ignore_index=True,
                                                  sort=True))
                if not df.empty:
                    return NodeStreamDF(df)
        else:
            raise UnrecognizedNodeStream('second operand')
        return NodeStreamDF()

    def __or__(self, ns):
        if isinstance(ns, API.NodeStream):
            if not bool(self):
                return ns.copy()
            if ns:
                if isinstance(ns, NodeStreamB):
                    nst, tdf = ns.nodeset, TimeSetDF(ns.timeset).df
                    df = self.df[~self.df.u.isin(nst)].append(
                            utils.merge_intervals_with_df(
                                self.df[self.df.u.isin(nst)],
                                tdf), ignore_index=True)
                    nstd = nst - self.nodeset
                    if bool(nstd):
                        df = df.append(pd.DataFrame(
                            list((n, ts, tf) for n in nstd for ts, tf in tdf),
                            columns=['u', 'ts', 'tf']),
                                  ignore_index=True)
                    return NodeStreamDF(df)
                elif not isinstance(ns, NodeStreamDF):
                    try:
                        return ns | self
                    except NotImplementedError:
                        pass
                return NodeStreamDF(utils.merge_intervals_df(
                        self.df.append(utils.ns_to_df(ns), ignore_index=True)))
            else:
                return self.copy()
        else:
            raise UnrecognizedNodeStream('second operand')
        return NodeStreamDF()

    def __sub__(self, ns):
        if isinstance(ns, API.NodeStream):
            if bool(self):
                if ns:
                    if isinstance(ns, NodeStreamB):
                        df = self.df[~self.df.u.isin(ns.nodeset)].append(
                            utils.difference_with_df(
                                self.df[self.df.u.isin(ns.nodeset)],
                                TimeSetDF(ns.timeset).df))
                    else:
                        df = utils.difference_df(self.df, utils.ns_to_df(ns))
                    return NodeStreamDF(df)
                else:
                    return self.copy()
        else:
            raise UnrecognizedNodeStream('second operand')
        return NodeStreamDF()

    def node_duration(self, u):
        return utils.measure_time(self.df[self.df == u])

    def common_time(self, u, v=None):
        if v is None:
            idx = self.df['u'] == u
            a, b = self.df[idx], self.df[~ idx]
            return utils.interval_intersection_size(a, b)
        else:
            return utils.interval_intersection_size(self.df[self.df.u == u], self.df[self.df.u == v])

    def number_of_nodes_at(self,  t):
        return utils.df_count_at(self.df, t)

    def nodes_at(self, ts):
        if isinstance(ts, API.TimeSet):
            return NodeSetS(utils.common_time_with_df(self.df, TimeSetDF(ts).df).v.itertuples(index=False, name=None))
        else:
            return NodeSetS(utils.df_at(self.df, ts).v.itertuples(index=False, name=None))

    def times_of(self, u):
        return TimeSetDF(self.df[self.df.u == u].drop(columns=['u']), disjoint_intervals=False)

    def issuperset(self, ns):
        if isinstance(ns, API.NodeStream):
            if not bool(self):
                return False
            elif bool(ns):
                if isinstance(ns, NodeStreamB):
                    nst = ns.nodeset
                    if nst.issuperset(self.nodeset):
                        return utils.issuper_with_df(self.df[self.u.isin(nst)],
                                                     TimeSetDF(ns.timeset).df)
                else:
                    return not ns or utils.issuper_df(self.df, utils.ns_to_df(ns))
        else:
            raise UnrecognizedNodeStream('ns')
        return False
