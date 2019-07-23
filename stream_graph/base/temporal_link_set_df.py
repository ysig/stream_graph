from __future__ import absolute_import
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

from .utils import ts_to_df, tns_to_df, make_algebra
from .multi_df_utils import load_interval_wdf, init_interval_df, build_time_generator, itertuples_pretty, itertuples_raw
from .multi_df_utils import set_unweighted_n_sparse, set_weighted_links_, set_unweighted_links_, get_key_first, len_set_n, len_set_, sum_counter_, sum_counter_n
from .functions import get_maximal_cliques as get_maximal_cliques_

from stream_graph import ABC
from .link_set_df import LinkSetDF
from .time_set_df import TimeSetDF
from .node_set_s import NodeSetS
from .temporal_node_set_df import TemporalNodeSetDF
from .temporal_node_set_b import TemporalNodeSetB
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
    """DataFrame implementation of ABC.TemporalLinkSet

    Parameters
    ----------
    df: pandas.DataFrame or Iterable, default=None
        If a DataFrame it should contain four columns for u and v and ts, tf.
        If an Iterable it should produce :code:`(u, v, ts, tf)` tuples of two NodeId (int or str) and two timestamps (Real) with :code:`ts < tf`.

    disjoint_intervals: Bool, default=False
        Defines if for each link all intervals are disjoint.

    sort_by: Any non-empty subset of ['u', 'v', 'ts', 'tf'].
        The order of the DataFrame elements by which they will be produced when iterated.

    discrete : bool, or default=None.

    weighted : bool, or default=None.

    default_closed : {'left', 'right', 'both', 'neither'}, default=None 

    merge_function : A function applied to a list of arguments.

    operation_functions: dict {str: fun}
        A dictionary of names of operations, i.e. :code:`union/u`, :code:`intersection/i`, :code:`difference/d`, :code:`issuperset/s'.
        All function should be applicable between two weights.
        Default: +, min, hinge_loss (ignoring an interval on zero), operator.ge (ignoring an interval on zero)

    """
    def __init__(self, df=None, disjoint_intervals=True, sort_by=None, discrete=None, weighted=False, default_closed=None, merge_function=None, operation_functions=None):
        if isinstance(df, self.__class__):
            if bool(df):
                self.df_, self.discrete_, self.weighted_, self.sort_by, self.algebra = df.df, df.discrete, df.weighted, df.sort_by, df.algebra
        elif df is not None:
            if isinstance(df, ABC.TemporalLinkSet):
                from .itemporal_link_set_df import ITemporalLinkSetDF
                if isinstance(df, ITemporalLinkSetDF):
                    df = df.df
                else:
                    weighted, discrete = df.weighted, df.discrete
            self.df_, self.discrete_, self.weighted_ = load_interval_wdf(df, discrete, weighted, disjoint_intervals, default_closed, keys=['u', 'v'], merge_function=merge_function)
            self.sort_by = sort_by
            if self.weighted_:
                self.algebra = make_algebra(operation_functions)
        else:
            self.discrete_, self.weighted_ = (True if discrete is None else discrete), weighted

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
            self.sorted_ = True
        return self

    def _empty_base_class(self):
        return init_interval_df(discrete=self.discrete, weighted=self.weighted, keys=['u'])

    @property
    def sorted_df(self):
        if bool(self):
            return self.sort_.df_
        else:
            return self._empty_base_class()

    @property
    def df(self):
        if bool(self):
            return self.df_
        else:
            return self._empty_base_class()

    def sort_df(self, sort_by):
        if self.sort_by is None:
            self.sort_by = sort_by
            return self.sort_df(sort_by)
        elif self.sort_by == sort_by:
            return self.sorted_df
        else:
            return self.df_.sort_values(by=self.sort_by, inplace=True)

    @property
    def size(self):
        if bool(self):
            return self.df.measure_time()
        else:
            return 0

    @property
    def _weighted_size(self):
        return self.df.measure_time(weights=True)

    @property
    def basic_temporal_nodeset(self):
        # Create node stream
        if bool(self):
            return TemporalNodeSetB(self.nodeset, TimeSetDF([self.df.limits], discrete=self.discrete))
        else:
            return TemporalNodeSetB()

    @property
    def minimal_temporal_nodeset(self):
        # All the time intervals that a node belongs to a link
        if bool(self):
            mdf = self.df.drop(columns=['u'], merge=False).rename(columns={'v': 'u'}).append(self.df.drop(columns=['v'], merge=False), merge=True)
            return TemporalNodeSetDF(mdf)
        else:
            return TemporalNodeSetDF()

    @property
    def linkset(self):
        if bool(self):
            if self.weighted:
                return LinkSetDF(self.df[['u', 'v', 'w']], no_duplicates=False, weighted=True)
            else:
                return LinkSetDF(self.df[['u', 'v']].drop_duplicates())
        else:
            return LinkSetDF()

    @property
    def timeset(self):
        if bool(self):
            return TimeSetDF(self.df.drop(columns=['u', 'v'], merge=True), discrete=self.discrete)
        else:
            return TimeSetDF()

    @property
    def nodeset(self):
        if bool(self):
            return NodeSetS(self.df.v.drop_duplicates().append(self.df.u.drop_duplicates(), ignore_index=True))
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
            if isinstance(t, tuple):
                if self.discrete:
                    df = self.df[reduce(operator.__and__, lpd)]
                    if self.weighted:
                        if len(t) == 2:
                            return df.issuper(init_interval_df(data=[t], discrete=True), by_key=False, issuper_function='unweighted')
                        else:
                            assert len(t) == 3
                            return df.issuper(init_interval_df(data=[t], weighted=True, discrete=True), by_key=False)
                    else:
                        lpd.append(self.df.index_at_interval(*t))
                else:
                    lpd.append(self.df.index_at_interval(*t))
            elif isinstance(t, Real):
                lpd.append(self.df.index_at(t))
            else:
                raise ValueError('Input can either be a real number or an ascending interval of two real numbers')
        return reduce(operator.__and__, lpd).any()

    def __iter__(self):
        if bool(self):
            return itertuples_pretty(self.df, discrete=self.discrete, weighted=self.weighted)
        else:
            return iter([])

    def _build_time_generator(self, cache_constructor, calculate, weighted=None, df=None, **kargs):
        if df is None:
            df = self.df
        return build_time_generator(df, cache_constructor=cache_constructor, calculate=calculate, **kargs)

    @property
    def _wc(self):
        return ['w'] if self.weighted else []

    # Maybe turn TimeCollection to boundy
    def links_at(self, t=None):
        if not bool(self):
            if t is None:
                return iter()
            else:
                return LinkSetDF()
        if t is None:
            calculate = (set_weighted_links_ if self.weighted else set_unweighted_links_)
            return TimeGenerator(((e, LinkSetDF(s, weighted=self.weighted)) for e, s in self._build_time_generator(set, calculate, add_weights=False)), discrete=self.discrete)
        elif isinstance(t, tuple):
            return LinkSetDF(self.df.df_at_interval(*t)[['u', 'v'] + self._wc], weighted=self.weighted)
        else:
            return LinkSetDF(self.df.df_at(t)[['u', 'v'] + self._wc], weighted=self.weighted)

    def times_of(self, l=None, direction='out'):
        if not bool(self):
            if l is None:
                return LinkCollection()
            else:
                return TimeSetDF()
        if l is None:
            times = defaultdict(list)
            di = False
            if direction == 'out':
                def add(key, time):
                    times[key].append(time)
            elif direction == 'in':
                def add(key, time):
                    times[key].append(time)
            elif direction == 'both':
                def add(key, time):
                    times[tuple(sorted(key))].append(time)
                di = True
            else:
                raise UnrecognizedDirection()
            for key in self.df.itertuples(**({} if self.discrete else {'bounds': True})):
                add(key[:2], key[2:])
            return LinkCollection({l: TimeSetDF(init_interval_df(data=ts, discrete=self.discrete, disjoint_intervals=not (di or self.weighted))) for l, ts in iteritems(times)})
        else:
            mf, kw, = False, (['w'] if self.weighted else [])
            u, v = l
            if direction == 'out':
                df = self.df[(self.df.u == u) & (self.df.v == v)]
            elif direction == 'in':
                df = self.df[(self.df.v == u) & (self.df.u == v)]
            elif direction == 'both':
                df, mf = self.df[self.df.u.isin({u, v}) & self.df.v.isin({u, v})], True
            else:
                raise UnrecognizedDirection()
            return TimeSetDF(df.drop(columns=['u', 'v']+kw, merge=(mf or self.weighted)), discrete=self.discrete)

    def duration_of(self, l=None, weights=False, direction='out'):
        if not bool(self):
            if l is None:
                return LinkCollection()
            else:
                return .0
        weighted = self.weighted and weights

        if l is None:
            times = defaultdict(float)
            dc = (1 if self.discrete else 0)
            if direction == 'out':
                def add(u, v, ts, tf, w=1):
                    times[(u, v)] += (tf - ts + dc)*w
            elif direction == 'in':
                def add(u, v, ts, tf, w=1):
                    times[(v, u)] += (tf - ts + dc)*w
            elif direction == 'both':
                def add(u, v, ts, tf, w=1):
                    times[tuple(sorted([u, v]))] += (tf - ts + dc)*w
            else:
                raise UnrecognizedDirection()
            for args in itertuples_raw(self.df, discrete=True, weighted=weighted):
                add(*args)

            return LinkCollection({l: ts for l, ts in iteritems(times)})
        else:
            di = False
            u, v = l
            if direction == 'out':
                df = self.df[(self.df.u == u) & (self.df.v == v)]
            elif direction == 'in':
                df = self.df[(self.df.v == u) & (self.df.u == v)]
            elif direction == 'both':
                df, di = self.df[self.df.u.isin({u, v}) & self.df.v.isin({u, v})], True
            else:
                raise UnrecognizedDirection()

            if weighted:
                return df.drop(columns=['u', 'v'], merge=di).measure_time(weights=True)
            else:
                return df.drop(columns=['u', 'v'] + self._wc, merge=di).measure_time()

    def neighbors_at(self, u=None, t=None, direction='out'):
        if not bool(self):
            if u is None:
                return NodeCollection()
            if t is None:
                return TimeCollection()
            return NodeSetS()

        if u is None:
            if t is None:
                out = dict()
                for u, val in self._build_time_generator(set, set_unweighted_n_sparse, weighted=False, direction=direction, get_key=get_key_first, sparse=True):
                    d = out.get(u, None)
                    if d is None:
                        out[u] = TimeSparseCollection([val], discrete=self.discrete, caster=NodeSetS)
                    else:
                        d.append(val)

                return NodeCollection(out)
            else:
                return LinkSetDF(self.df.df_at(t)[['u', 'v'] + self._wc]).neighbors_of(u=None, direction=direction)
        else:
            if direction == 'out':
                df = self.df[self.df.u == u].drop(columns=['u']+self._wc, merge=False).rename(columns={'v': 'u'})
            elif direction == 'in':
                df = self.df[self.df.v == u].drop(columns=['v']+self._wc, merge=False)
            elif direction == 'both':
                df = self.df[self.df.u == u].drop(columns=['u']+self._wc).rename(columns={'v': 'u'})
                df = df.append(self.df[self.df.v == u].drop(columns=['v']+self._wc, merge=True), ignore_index=True)
            else:
                raise UnrecognizedDirection()
            if t is None:
                return TemporalNodeSetDF(df).nodes_at(t=None)
            else:
                return NodeSetS(df[df.index_at(t)].u.values.flat)

    def _degree_at_weighted(self, u, t, direction):
        if u is None:
            if t is None:
                out = dict()
                for u, val in self._build_time_generator(Counter, sum_counter_n, direction=direction, get_key=get_key_first):
                    d = out.get(u, None)
                    if d is None:
                        out[u] = TimeCollection([val], discrete=self.discrete, instantaneous=False)
                    else:
                        d.append(val)
                return NodeCollection(out)
            else:
                return LinkSetDF(self.df.df_at(t)[['u', 'v', 'w']], weighted=True).degree(u=None, direction=direction, weights=True)
        else:
            if direction == 'out':
                df = self.df[self.df.u == u].drop(columns=['u'], merge=False).rename(columns={'v': 'u'})
            elif direction == 'in':
                df = self.df[self.df.v == u].drop(columns=['v'], merge=False)
            elif direction == 'both':
                df = self.df[self.df.u == u].drop(columns=['u'], merge=False).rename(columns={'v': 'u'})
                df = df.append(self.df[self.df.v == u].drop(columns=['v'], merge=True), ignore_index=True)
            else:
                raise UnrecognizedDirection()
            if t is None:
                return TimeCollection(self._build_time_generator(Counter, sum_counter_, direction=direction, df=df), discrete=self.discrete, instantaneous=False)
            else:
                return df.w[df.index_at(t)].sum()

    def _degree_at_unweighted(self, u, t, direction):
        if u is None:
            if t is None:
                out = dict()
                for u, val in self._build_time_generator(set, len_set_n, direction=direction, get_key=get_key_first):
                    d = out.get(u, None)
                    if d is None:
                        out[u] = TimeCollection([val], discrete=self.discrete, instantaneous=False)
                    else:
                        d.append(val)
                return NodeCollection(out)
            else:
                return LinkSetDF(self.df.df_at(t)[['u', 'v'] + self._wc]).degree(u=None, direction=direction)
        else:
            df = (self.df.drop(columns='w', merge=False) if self.weighted else self.df)
            if direction == 'out':
                df = df[df.u == u].drop(columns=['u'], merge=self.weighted).rename(columns={'v': 'u'})
            elif direction == 'in':
                df = df[df.v == u].drop(columns=['v'], merge=self.weighted)
            elif direction == 'both':
                dfa = df[df.u == u].drop(columns=['u'], merge=False).rename(columns={'v': 'u'})
                df = dfa.append(df[df.v == u].drop(columns=['v'], merge=False), ignore_index=True, merge=True)
            else:
                raise UnrecognizedDirection()
            if t is None:
                return TemporalNodeSetDF(df).n_at(t=None)
            else:
                return len(set(df.df_at(t).u.values.flat))

    def neighbors_of(self, u=None, direction='out'):
        if not bool(self):
            if u is None:
                return dict()
            else:
                return TemporalNodeSetDF()
        if u is None:
            neighbors = defaultdict(list)
            if direction == 'out':
                def add(key):
                    neighbors[key[0]].append(key[1:])
            elif direction == 'in':
                def add(key):
                    neighbors[key[1]].append((key[0],) + key[2:])
            elif direction == 'both':
                def add(key):
                    neighbors[key[0]].append(key[1:])
                    neighbors[key[1]].append((key[0],) + key[2:])
            else:
                raise UnrecognizedDirection()
            for key in itertuples_raw(self.df, discrete=self.discrete, weighted=False):
                add(key)
            return NodeCollection({u: TemporalNodeSetDF(init_interval_df(data=ns, discrete=self.discrete, weighted=False,  disjoint_intervals=False, keys=['u'])) for u, ns in iteritems(neighbors)})
        else:
            if direction == 'out':
                df = self.df[self.df.u == u].drop(columns=['u'], merge=False).rename(columns={'v': 'u'})
            elif direction == 'in':
                df = self.df[self.df.v == u].drop(columns=['v'], merge=False)
            elif direction == 'both':
                df = self.df[self.df.u == u].drop(columns=['u'], merge=False).rename(columns={'v': 'u'})
                df = df.append(self.df[self.df.v == u].drop(columns=['v'], merge=False), ignore_index=True, merge=False)
            else:
                raise UnrecognizedDirection()
            if self.weighted:
                df = df.drop(columns='w', merge=False)
            return TemporalNodeSetDF(df, disjoint_intervals=False)

    def degree_of(self, u=None, direction='out', weights=False):
        if not bool(self):
            if u is None:
                return dict()
            else:
                return TemporalNodeSetDF()

        if u is None:
            degree = Counter()
            dc = (1 if self.discrete else 0)
            if direction == 'out':
                def add(u, v, ts, tf, w=1):
                    degree[u] += (tf - ts + dc)*w
            elif direction == 'in':
                def add(u, v, ts, tf, w=1):
                    degree[v] += (tf - ts + dc)*w
            elif direction == 'both':
                def add(u, v, ts, tf, w=1):
                    degree[u] += (tf - ts + dc)*w
                    degree[v] += (tf - ts + dc)*w
            else:
                raise UnrecognizedDirection()

            kargs = ({'weights': True} if self.weighted and weights else {})
            for key in self.df.itertuples(**kargs):
                add(*key)

            return NodeCollection(degree)
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

            if self.weighted and weights:
                return df.w.sum()
            else:
                df = (df.drop(columns=['w'], merge=False) if self.weighted else df)
                return TemporalNodeSetDF(df, disjoint_intervals=False, discrete=self.discrete).size

    def substream(self, nsu, nsv, ts):
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
                    ts = TimeSetDF(ts, discrete=self.discrete)
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
                df = self

            if ts is not None:
                if self.weighted:
                    df = df.intersection(ts_to_df(ts), by_key=False, on_column=['u', 'v'], intersection_function='unweighted')
                else:
                    df = df.intersection(ts_to_df(ts), by_key=False, on_column=['u', 'v'])
            return self.__class__(df, discrete=self.discrete, weighted=self.weighted)
        else:
            return self.__class__()

    def __and__(self, tls):
        if isinstance(tls, ABC.TemporalLinkSet):
            if bool(tls) and bool(self):
                assert tls.discrete == self.discrete
                if not isinstance(tls, TemporalLinkSetDF):
                    try:
                        return tls & self
                    except NotImplementedError:
                        tls = TemporalLinkSetDF(tls, discrete=self.discrete, weighted=self.weighted)
                out = (self.df.intersection(tls.df, intersection_function=self.algebra['i']) if self.weighted else self.df.intersection(tls.df))
                return TemporalLinkSetDF(out, discrete=self.discrete, weighted=self.weighted)
        else:
            raise UnrecognizedTemporalLinkSet('right operand')
        return TemporalLinkSetDF(discrete=self.discrete, weighted=self.weighted)

    def __or__(self, tls):
        if isinstance(tls, ABC.TemporalLinkSet):
            if not bool(self):
                return tls.copy()
            elif bool(tls):
                assert tls.discrete == self.discrete
                if not isinstance(tls, TemporalLinkSetDF):
                    try:
                        return tls | self
                    except NotImplementedError:
                        tls = TemporalLinkSetDF(tls, discrete=self.discrete, weighted=self.weighted)
                out = (self.df.union(tls.df, union_function=self.algebra['u']) if self.weighted else self.df.union(tls.df))
                return TemporalLinkSetDF(out, discrete=self.discrete, weighted=self.weighted)
            else:
                return self.copy()
        else:
            raise UnrecognizedTemporalLinkSet('right operand')

    def __sub__(self, tls):
        if isinstance(tls, ABC.TemporalLinkSet):
            if bool(self) and bool(tls):
                assert tls.discrete == self.discrete
                if isinstance(tls, LinkSetDF):
                    try:
                        return tls.__rsub__(self)
                    except (AttributeError, NotImplementedError):
                        tls = LinkSetDF(tls, discrete=self.discrete, weighted=self.weighted)
                out = (self.df.difference(tls.df, difference_function=self.algebra['d']) if self.weighted else self.df.difference(tls.df))
                return TemporalLinkSetDF(out, discrete=self.discrete, weighted=self.weighted)
        else:
            raise UnrecognizedTemporalLinkSet('right operand')
        return self.copy()

    def issuperset(self, tls):
        if isinstance(tls, ABC.TemporalLinkSet):
            if bool(self) and bool(tls):
                assert tls.discrete == self.discrete
                if not isinstance(tls, TemporalLinkSetDF):
                    try:
                        return tls.__issubset__(self)
                    except (AttributeError, NotImplementedError):
                        tls = TemporalLinkSetDF(tls, discrete=self.discrete)
                return (self.df.issuper(tls.df, issuper_function=self.algebra['s']) if self.weighted else self.df.issuper(tls.df))
            else:
                return not bool(tls)
        else:
            raise UnrecognizedTemporalLinkSet('ls')
        return TemporalLinkSetDF()

    def _m_at_unweighted(self, t):
        if t is None:
            return TimeCollection(self._build_time_generator(set, len_set_, weighted=False), instantaneous=False, discrete=self.discrete)
        else:
            return self.df.count_at(t)

    def _m_at_weighted(self, t):
        if t is None:
            return TimeCollection(self._build_time_generator(Counter, sum_counter_), instantaneous=False, discrete=self.discrete)
        else:
            return self.df.count_at(t, weights=True)

    def temporal_neighborhood(self, tns, direction='out'):
        # if df join on u / combine (intersect) and the union intervals (for union)
        # if range
        derror = False
        if not isinstance(tns, ABC.TemporalNodeSet):
            raise UnrecognizedTemporalNodeSet('ns')
        if isinstance(tns, TemporalNodeSetB):
            if direction == 'out':
                df = self.df.rename(columns={'v': 'u', 'u': 'v'})
                df = df[df.v.isin(tns.nodeset_)].drop('v', axis=1)
            elif direction == 'in':
                df = self.df[self.df.v.isin(tns.nodeset_)].drop('v', axis=1)
            elif direction == 'both':
                df = self.df.rename(columns={'v': 'u', 'u': 'v'})
                df = df[df.v.isin(tns.nodeset_)].drop('v', axis=1, merge=False)
                df = df.append(self.df[self.df.v.isin(tns.nodeset_)].drop('v', axis=1, merge=False), merge=True)
            else:
                derror = True
            if not derror:
                df = df.intersection(ts_to_df(tns.timeset_), on_columns=['u', 'v'], by_key=False)
        else:
            base_df = tns_to_df(tns)
            if direction == 'out':
                df = self.df
            elif direction == 'in':
                df = self.df.rename(columns={'u': 'v', 'v': 'u'})
            elif direction == 'both':
                df = self.df.append(self.df.rename(columns={'u': 'v', 'v': 'u'}), ignore_index=True, merge=True)
            else:
                derror = True
            df = df.map_intersection(base_df)
        if derror:
            raise UnrecognizedDirection()
        return TemporalNodeSetDF(df, disjoint_intervals=False, discrete=self.discrete)

    def induced_substream(self, tns):
        if isinstance(tns, ABC.TemporalNodeSet):
            if bool(self) and bool(tns):
                assert tns.discrete == self.discrete
                if isinstance(tns, TemporalNodeSetB):
                    tdf = self.df_[self.df_['v'].isin(tns.nodeset_) & self.df_['u'].isin(tns.nodeset_)]
                    if self.weighted:
                        tdf = tdf.intersection(ts_to_df(tns.timeset_), on_columns=['u', 'v'], by_key=False)
                    else:
                        tdf = tdf.intersection(ts_to_df(tns.timeset_), on_columns=['u', 'v'], by_key=False, intersection='unweighted')
                    if not tdf.empty:
                        return TemporalLinkSetDF(tdf, discrete=self.discrete, weighted=self.weighted)
                else:
                    base_df = tns_to_df(tns)
                    out = (self.df_.cartesian_intersection(base_df, cartesian_intersection_function='unweighted') if self.weighted else self.df_.cartesian_intersection(base_df))
                    return TemporalLinkSetDF(out, discrete=self.discrete, weighted=self.weighted)
        else:
            raise UnrecognizedTemporalNodeSet('ns')
        return TemporalLinkSetDF(discrete=self.discrete, weighted=self.weighted)

    @property
    def m(self):
        if bool(self):
            return self.df_[['u', 'v']].drop_duplicates().shape[0]
        else:
            return 0

    def get_maximal_cliques(self, direction='both'):
        if bool(self):
            return get_maximal_cliques_(self.sort_df(sort_by=['ts', 'tf']), direction=direction)
        else:
            return set()

    # Make cliques for intervals?
    def _to_discrete(self, bins, bin_size):
        df, bins = time_discretizer_df(self.df, bins, bin_size, columns=['ts', 'tf'])
        return self.__class__(df, disjoint_intervals=False, discrete=True), bins
