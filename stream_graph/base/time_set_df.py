from __future__ import absolute_import
from warnings import warn
from numbers import Real
from .utils import time_discretizer_df
from .multi_df_utils import load_interval_df, itertuples_pretty, init_interval_df
from stream_graph import ABC
from stream_graph.exceptions import UnrecognizedTimeSet
from stream_graph.exceptions import UnrecognizedTemporalNodeSet


class TimeSetDF(ABC.TimeSet):
    """A DataFrame implementation of a TimeSet.

    Parameters
    ----------
    df: pandas.DataFrame or Iterable, default=None
        If a DataFrame it should contain two columns `ts`, `tf` and optionally a column `itype` of elements 'right', 'left', 'both', 'neither' that indicate.
        If an Iterable it should produce :code:`(ts, tf)` tuples of two timestamps (Real) with :code:`ts < tf` and optionally a third element 'right', 'left', 'both', 'neither' that indicates the interval-type.

    disjoint_intervals: Bool, default=False
        Defines if all intervals are disjoint.

    discrete : Bool or None, default=None

    default_closed : 'right', 'left', 'both', 'neither', default='left'

    """
    def __init__(self, df=None, disjoint_intervals=True, discrete=None, default_closed='both'):
        # Add a check for dataframe style
        if df is not None:
            if isinstance(df, ABC.TimeSet):
                self.df_, self.discrete_ = df, df.discrete
            else:
                if isinstance(df, ABC.ITimeSet):
                    self.discrete = discrete
                self.df_, self.discrete_ = load_interval_df(df, discrete=discrete, default_closed=default_closed, disjoint_intervals=disjoint_intervals)
            if not self.df_.empty and discrete and self.df_['ts'].dtype.kind != 'i' and self.df_['tf'].dtype.kind != 'i':
                warn('SemanticWarning: For a discrete instance time-instants should be an integers')
        else:
            self.discrete_ = (True if discrete is None else discrete)

    @property
    def discrete(self):
        return self.discrete_

    @property
    def is_sorted_(self):
        return hasattr(self, 'sorted_') and self.sorted_

    @property
    def sort_df_(self):
        if not self.is_sorted_:
            self.df_.sort_values(by=['ts'], inplace=True)
            self.sorted_ = True
        return self

    @property
    def is_merged_(self):
        return hasattr(self, 'merged_') and self.merged_

    def _empty_base_class(self):
        return init_interval_df(self.discrete, keys=[''])

    @property
    def sorted_df(self):
        if bool(self):
            return self.sort_df_.df_
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
    def df(self):
        if bool(self):
            return self.df_
        else:
            return self._empty_base_class()

    def __bool__(self):
        return hasattr(self, 'df_') and not self.df_.empty

    @property
    def size(self):
        if bool(self):
            return self.df.measure_time()
        else:
            return 0

    def __contains__(self, t):
        if bool(self):
            if isinstance(t, tuple):
                return self.df_.index_at_interval(*t).any()
            elif isinstance(t, Real):
                return self.df_.index_at(t).any()
            else:
                raise ValueError('Input can either be a real number or an ascending interval of two real numbers')
        else:
            return False

    def __and__(self, ts):
        if isinstance(ts, ABC.TimeSet):
            assert self.discrete == ts.discrete
            if bool(self) and bool(ts):
                if not isinstance(ts, TimeSetDF):
                    try:
                        return ts & self
                    except NotImplementedError:
                        ts = self.__class__(ts)
                return self.__class__(self.df.intersection(ts.df))
        else:
            raise UnrecognizedTimeSet('right operand')
        return self.__class__()

    def __or__(self, ts):
        if isinstance(ts, ABC.TimeSet):
            assert self.discrete == ts.discrete
            if not bool(self):
                return ts.copy()
            if bool(ts):
                if not isinstance(ts, TimeSetDF):
                    try:
                        return ts | self
                    except NotImplementedError:
                        ts = self.__class__(ts)
                return self.__class__(self.df.union(ts.df))
            else:
                return self.copy()
        else:
            raise UnrecognizedTimeSet('right operand')

    def __sub__(self, ts):
        if isinstance(ts, ABC.TimeSet):
            assert self.discrete == ts.discrete
            if bool(self) and bool(ts):
                if not isinstance(ts, TimeSetDF):
                    try:
                        return ts.__rsub__(self)
                    except (AttributeError, NotImplementedError):
                        ts = self.__class__(ts)
                return self.__class__(self.df.difference(ts.df))
        else:
            raise UnrecognizedTimeSet('right operand')
        return self.copy()

    def __iter__(self):
        if bool(self):
            return itertuples_pretty(self.df, self.discrete)
        else:
            return iter([])

    def issuperset(self, ts):
        if isinstance(ts, ABC.TimeSet):
            assert ts.discrete == self.discrete
            if bool(ts):
                if not isinstance(ts, self.__class__):
                    ts = self.__class__(ts)
                return not bool(ts) or self.df.issuper(ts.df)
        else:
            raise UnrecognizedTemporalNodeSet('ns')
        return False

    def _to_discrete(self, bins, bin_size):
        df, bins = time_discretizer_df(self.df, bins, bin_size, columns=['ts', 'tf'])
        return self.__class__(df, disjoint_intervals=False, discrete=True), bins
