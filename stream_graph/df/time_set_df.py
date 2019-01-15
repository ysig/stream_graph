from numbers import Real

import pandas as pd

from .interval_df import IntervalDF
from stream_graph import ABC
from stream_graph.exceptions import UnrecognizedTimeSet


class TimeSetDF(ABC.TimeSet):
    def __init__(self, df=None, disjoint_intervals=True):
        # Add a check for dataframe style
        if df is not None:
            if isinstance(df, IntervalDF):
                self.df_ = df
            elif isinstance(df, pd.DataFrame):
                self.df_ = IntervalDF(df, columns=['ts', 'tf'])
            elif hasattr(df, '__iter__'):
                self.df_ = IntervalDF(list(df), columns=['ts', 'tf'])
            else:
                raise ValueError('Input must be an iterable')
            self.merged_ = disjoint_intervals

    @property
    def is_merged_(self):
        return hasattr(self, 'merged_') and self.merged_

    @property
    def df(self):
        if bool(self):
            if not self.is_merged_:
                self.df_.union(inplace=True)
                self.merged_ = True
            return self.df_
        else:
            return IntervalDF(columns=['ts', 'tf'])

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
            if bool(self) and bool(ts):
                if not isinstance(ts, TimeSetDF):
                    try:
                        return ts & self
                    except NotImplementedError:
                        ts = TimeSetDF(ts)
                return TimeSetDF(self.df.intersect(ts.df))
        else:
            raise UnrecognizedTimeSet('right operand')
        return TimeSetDF()

    def __or__(self, ts):
        if isinstance(ts, ABC.TimeSet):
            if not bool(self):
                return ts.copy()
            if bool(ts):
                if not isinstance(ts, TimeSetDF):
                    try:
                        return ts | self
                    except NotImplementedError:
                        ts = TimeSetDF(ts)
                return TimeSetDF(self.df.union(ts.df))
            else:
                return self.copy()
        else:
            raise UnrecognizedTimeSet('right operand')

    def __sub__(self, ts):
        if isinstance(ts, ABC.TimeSet):
            if bool(self) and bool(ts):
                if not isinstance(ts, TimeSetDF):
                    try:
                        return ts.__rsub__(self)
                    except (AttributeError, NotImplementedError):
                        ts = TimeSetDF(ts)
                return TimeSetDF(self.df.difference(ts.df))
        else:
            raise UnrecognizedTimeSet('right operand')
        return self.copy()

    def __iter__(self):
        return self.df.itertuples(index=False, name=None)

    def issuperset(self, ts):
        if not isinstance(ts, TimeSetDF):
            ts = TimeSetDF(ts)
        return self.df.issuper(ts.df)
