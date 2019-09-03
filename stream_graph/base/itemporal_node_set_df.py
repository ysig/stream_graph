from __future__ import absolute_import
from numbers import Real
from warnings import warn
from collections import defaultdict
from collections import Counter
from six import iteritems
from itertools import combinations

from . import utils
from .utils import time_discretizer_df
from stream_graph import ABC
from .multi_df_utils import init_instantaneous_df, load_instantaneous_df
from .node_set_s import NodeSetS
from .itime_set_s import ITimeSetS
from stream_graph.exceptions import UnrecognizedTemporalNodeSet
from stream_graph.collections import TimeCollection
from stream_graph.collections import TimeGenerator
from stream_graph.collections import NodeCollection
from stream_graph.collections import LinkCollection


class ITemporalNodeSetDF(ABC.ITemporalNodeSet):
    """DataFrame implementation of ABC.ITemporalNodeSet

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
    def __init__(self, df=None, no_duplicates=True, sort_by= None, discrete=None):
        if isinstance(df, self.__class__):
            discrete = df.discrete_
            self.df_ = df.df
        else:
            df, _ = load_instantaneous_df(df, no_duplicates=no_duplicates, weighted=False, keys=['u'])
            if df is not None:
                self.df_ = df
                self.sort_by = sort_by
                if isinstance(df, self.__class__):
                    # Extract discrete if it is already there
                    discrete = df.discrete
                if discrete is None:
                    # By default it is None
                    discrete = False
                if discrete and self.df_['ts'].dtype.kind != 'i' and self.df_['tf'].dtype.kind != 'i':
                    warn('SemanticWarning: For a discrete instance time-instants should be an integers')
        # If dscrete is not initialized, set it to the default value 1.
        self.discrete_ = True if discrete is None else discrete

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

    def sort_df(self, sort_by):
        """Return a sorted version of the data-frame given an order or retrieves it if already sorted."""
        if sort_by is None:
            return self.df
        elif self.sort_by is None:
            self.sort_by = sort_by
            return self.sort_df(sort_by)
        elif self.sort_by == sort_by:
            return self.sorted_df
        else:
            return self.df_.sort_values(by=sort_by)

    @property
    def sorted_df(self):
        if bool(self):
            return self.sort_.df_
        else:
            return self._empty_base_class()

    @property
    def sort_(self):
        if not self.is_sorted_:
            self.df_.sort_values(by=self.sort_by, inplace=True)
        return self

    def _empty_base_class(self):
        return init_instantaneous_df(keys=['u'])

    @property
    def nodeset(self):
        if not bool(self):
            return NodeSetS()
        return NodeSetS(self.df_.u.drop_duplicates())

    @property
    def df(self):
        if bool(self):
            return self.df_
        else:
            return self._empty_base_class()

    @property
    def timeset(self):
        if not bool(self):
            return ITimeSetS()
        return ITimeSetS(self.df_.ts, discrete=self.discrete)

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
                            # If not an instance of this class,
                            # see if the other class has a method.
                            return ns & self
                        except NotImplementedError:
                            pass
                    # Or else convert from its iterator form.
                    df = utils.ins_to_idf(ns).intersection(self.df)
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
                            # If not an instance of this class,
                            # see if the other class has a method.
                            return ns | self
                        except NotImplementedError:
                            pass
                    # Or else convert from its iterator form.
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
                        # If we have an instantaneous object - take the difference, by extracting the df
                        return self.__class__(self.df.difference(utils.ins_to_idf(ns)), discrete=self.discrete)
                    else:
                        # If we don't have, convert our object to a non-instantaneous object,
                        # subtract and then by converting back to our to our original type, we will have our valid result.
                        df = self.df
                        df['tf'] = df['ts']
                        return self.__class__((TemporalNodeSetDF(df, discrete=self.discrete) - ns).df.drop(columns=['tf']), discrete=self.discrete)
                else:
                    return self.copy()
        else:
            raise UnrecognizedTemporalNodeSet('second operand')
        return self.__class__()

    def n_at(self, t=None):
        if bool(self):
            if t is None:
                # Count how many times its time-step occurs, for each key and return a sorted list 
                return TimeCollection(sorted(list(iteritems(Counter(t for t in self.df.ts)))), instantaneous=True, discrete=self.discrete)
            elif isinstance(t, Real):
                # Count for only one time-stamp
                return len(set(self.df.df_at(t).u))
            else:
                raise ValueError('Input can either be a real number or an ascending interval of two real numbers')
        else:
            if t is None:
                return TimeCollection(iter(), instantaneous=True, discrete=self.discrete)
            else:
                return NodeSetS()

    def nodes_at(self, t=None):
        if bool(self):
            if t is None:
                def generate(iter_):
                    prev = None
                    for u, ts in iter_:
                        if prev is None:
                            active_set, prev = {u}, ts
                        elif ts != prev:
                            yield (prev, NodeSetS(set(active_set)))
                            active_set, prev = {u}, ts
                        else:
                            active_set.add(u)
                    if len(active_set):
                        yield (prev, NodeSetS(set(active_set)))
                # Iterate in ascending time and yield the NodeSet at each time-instant in a generator fashion
                return TimeGenerator(generate(self.sort_df('ts').itertuples()), instantaneous=True, discrete=self.discrete)
            elif isinstance(t, Real):
                # Count how many times its time-step occurs, for each key and return a sorted list 
                return NodeSetS(self.df.df_at(t).u)
            else:
                raise ValueError('Input can either be a real number or an ascending interval of two real numbers')
        else:
            if t is None:
                return TimeCollection(iter(), instantaneous=True, discrete=self.discrete)
            else:
                return NodeSetS()

    def times_of(self, u=None):
        if bool(self):
            if u is None:
                times = defaultdict(set)
                for u, ts in iter(self):
                    times[u].add(ts)
                # Make a time-set for each node
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
                    from .temporal_link_set_df import TemporalNodeSetDF
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
            counter = Counter(ts for _, ts in iter(self))
            ct = sum(((val-1)*val)/2 for _, val in iteritems(counter) if val > 1)
        return ct

    @property
    def number_of_instants(self):
        return self.df.shape[0]

    def _node_duration_discrete(self, u=None):
        if u is None:
            return NodeCollection(Counter(u for u in self.df.u))
        else:
            if bool(self):
                return (self.df.u == u).sum()
            else:
                return 0

    def _common_time_discrete(self, u=None):
        if u is None or self._common_time__list_input(u):
            prev = None
            ct = defaultdict(int)
            if u is None:
                # If we want the common-time for all nodes
                for u, ts in self.sort_df('ts').itertuples():
                    if prev is None:
                        active_set, prev = {u}, ts
                    elif ts != prev:
                        if len(active_set) > 1:
                            for v in active_set:
                                # update their common time to the ammount of all the other coexisting nodes
                                ct[v] += (len(active_set)-1)
                        active_set, prev = {u}, ts
                    else:
                        active_set.add(u)
                if len(active_set) > 1:
                    for v in active_set:
                        # update their common time to the ammount of all the other coexisting nodes
                        ct[v] += (len(active_set)-1)
            else:
                # If we want the common-time for all nodes but only for a list of nodes
                for u, ts in self.sort_df('ts').itertuples():
                    if prev is None:
                        active_set, prev = {u}, ts
                    elif ts != prev:
                        if len(active_set) > 1:
                            for v in active_set:
                                # update their common time to the ammount of all the other coexisting nodes
                                if v in valid_nodes:
                                    ct[v] += (len(active_set)-1)
                        active_set, prev = {u}, ts
                    else:
                        active_set.add(u)
                if len(active_set) > 1:
                    for v in active_set:
                        # update their common time to the ammount of all the other coexisting nodes
                        if v in valid_nodes:
                            ct[v] += (len(active_set)-1)
            return NodeCollection(ct)
        else:
            if bool(self):
                idx = (self.df.u == u)
                if idx.any():
                    a, b = self.df[idx], self.df[~idx]
                    # For a single node this is equivalent to the amount
                    # of intersection between two groups of time-sets
                    return a.intersection_size(b)
            return 0.

    def _common_time_pair_discrete(self, l=None):
        if l is None or self._common_time_pair__list_input(l):
            carrier = defaultdict(set)
            # For each node take the set of time-stamps.
            for u, ts in iter(self):
                carrier[u].add(ts)
    
            # Take all the valid pairs of nodes.
            valid_links = (combinations(set(carrier.keys()), 2) if l is None else set(l))
            # And for all of them take the size of the intersection between all their time-sets.
            return LinkCollection({(u, v): len(carrier[u] & carrier[v]) for u, v in iter(valid_links)})
        else:
            u, v = l
            if bool(self):
                idxa, idxb = (self.df.u == u), (self.df.u == v)
                if idxa.any() and idxb.any():
                    return self.df[idxa].intersection_size(self.df[idxb])
                idxa, idxb = (self.df.u == u), (self.df.u == v)
                if idxa.any() and idxb.any():
                    return self.df[idxa].intersection_size(self.df[idxb])
            return 0.

    def _to_discrete(self, bins, bin_size):
        df, bins = time_discretizer_df(self.df, bins, bin_size, columns=['ts'])
        return self.__class__(df, no_duplicates=False, discrete=True), bins
