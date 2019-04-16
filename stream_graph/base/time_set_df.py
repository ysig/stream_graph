from warnings import warn
from numbers import Real

import pandas as pd

from .interval_df import IntervalDF
from stream_graph import ABC
from stream_graph.exceptions import UnrecognizedTimeSet


class TimeSetDF(ABC.TimeSet):
    """A DataFrame implementation of a TimeSet."""
    def __init__(self, df=None, disjoint_intervals=True, discrete=None):
        """Initialize a TimeSetDF.

        Parameters
        ----------
        df: pandas.DataFrame or Iterable, default=None
            If a DataFrame it should contain four columns for u and v and ts, tf.
            If an Iterable it should produce :code:`(ts, tf)` tuples of two timestamps (Real) with :code:`ts < tf`.

        disjoint_intervals: Bool, default=False
            Defines if all intervals are disjoint.

        discrete : Bool or None, default=None

        """
        # Add a check for dataframe style
        if df is not None:
            if isinstance(df, (ABC.TimeSet, ABC.ITimeSet)):
                discrete = df.discrete
            elif discrete is None:
                discrete = False
            self.discrete_ = discrete
            kargs = {'columns': ['ts', 'tf']} 
            if isinstance(df, ABC.ITimeSet):
                self.df_ = IntervalDF(list((ts, ts) for ts in df), **kargs)
            elif isinstance(df, (IntervalDF, pd.DataFrame)):
                self.df_ = IntervalDF(df, **kargs)
            elif hasattr(df, '__iter__'):
                self.df_ = IntervalDF(list(df), **kargs)
            else:
                raise ValueError('Input must be an iterable')
            if discrete and self.df_['ts'].dtype.kind != 'i' and self.df_['tf'].dtype.kind != 'i':
                warn('SemanticWarning: For a discrete instance time-instants should be an integers')
            self.merged_ = disjoint_intervals

    @property
    def discrete(self):
        if bool(self):
            return self.discrete_
        else:
            return None

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

    @property
    def df(self):
        if bool(self):
            if not self.is_merged_:
                self.df_.merge(inplace=True)
                self.merged_ = True
            return self.sort_df_.df_
        else:
            return IntervalDF(columns=['ts', 'tf'])

    def __bool__(self):
        return hasattr(self, 'df_') and not self.df_.empty

    @property
    def size(self):
        if bool(self):
            return self.df.measure_time(discrete=self.discrete_)
        else:
            return 0

    def __contains__(self, t):
        if bool(self):
            if isinstance(t, tuple) and len(t) == 2 and isinstance(t[0], Real) and isinstance(t[1], Real) and t[0]<=t[1]:
                ts, tf = t
                return self.df_.index_at_interval(ts, tf).any()
            elif isinstance(t, Real):
                return self.df_.index_at(t).any()
            else:
                raise ValueError('Input can either be a real number or an ascending interval of two real numbers')
        else:
            return False

    def __and__(self, ts):
        if isinstance(ts, ABC.TimeSet):
            assert ts.discrete == self.discrete
            if bool(self) and bool(ts):
                if not isinstance(ts, TimeSetDF):
                    try:
                        return ts & self
                    except NotImplementedError:
                        ts = self.__class__(ts)
                return self.__class__(self.df.intersect(ts.df), discrete=self.discrete)
        else:
            raise UnrecognizedTimeSet('right operand')
        return self.__class__()

    def __or__(self, ts):
        if isinstance(ts, ABC.TimeSet):
            assert ts.discrete == self.discrete
            if not bool(self):
                return ts.copy()
            if bool(ts):
                if not isinstance(ts, TimeSetDF):
                    try:
                        return ts | self
                    except NotImplementedError:
                        ts = self.__class__(ts)
                return self.__class__(self.df.union(ts.df), discrete=self.discrete)
            else:
                return self.copy()
        else:
            raise UnrecognizedTimeSet('right operand')

    def __sub__(self, ts):
        if isinstance(ts, ABC.TimeSet):
            assert ts.discrete == self.discrete
            if bool(self) and bool(ts):
                if not isinstance(ts, TimeSetDF):
                    try:
                        return ts.__rsub__(self)
                    except (AttributeError, NotImplementedError):
                        ts = self.__class__(ts, discrete=self.discrete)
                return self.__class__(self.df.difference(ts.df), discrete=self.discrete)
        else:
            raise UnrecognizedTimeSet('right operand')
        return self.copy()

    def __iter__(self):
        return self.df.itertuples(index=False, name=None)

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
