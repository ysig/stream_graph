import pandas as pd

from stream_graph import API
from stream_graph.df import utils
from stream_graph.exceptions import UnrecognizedTimeSet


class TimeSetDF(API.TimeSet):
    def __init__(self, df=None, disjoint_intervals=True):
        # Add a check for dataframe style
        if df is not None:
            if isinstance(df, pd.DataFrame):
                self.df_ = df
            elif hasattr(df, '__iter__'):
                self.df_ = pd.DataFrame(list(df), columns=['ts', 'tf'])
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
                self.df_ = utils.merge_intervals(self.df_)
                self.merged_ = True
            return self.df_
        else:
            return pd.DataFrame(columns=['ts', 'tf'])

    def __bool__(self):
        return hasattr(self, 'df_') and not self.df_.empty

    @property
    def size(self):
        if bool(self):
            return utils.measure_time(self.df_)
        else:
            return 0

    def __contains__(self, t):
        if bool(self):
            if isinstance(t, tuple) and len(t) == 2:
                ts, tf = t
                return utils.df_index_at_interval(self.df_, ts, tf).any()
            else:
                return utils.df_index_at(self.df_, t).any()
        else:
            return False

    def __and__(self, ts):
        if isinstance(ts, API.TimeSet):
            if bool(self) and bool(ts):
                if not isinstance(ts, TimeSetDF):
                    try:
                        return ts & self
                    except NotImplementedError:
                        ts = TimeSetDF(ts)
                return TimeSetDF(utils.intersect_intervals(self.df.append(ts.df, ignore_index=True)))
        else:
            raise UnrecognizedTimeSet('right operand')
        return TimeSetDF()

    def __or__(self, ts):
        if isinstance(ts, API.TimeSet):
            if not bool(self):
                return ts.copy()
            if bool(ts):
                if not isinstance(ts, TimeSetDF):
                    try:
                        return ts | self
                    except NotImplementedError:
                        ts = TimeSetDF(ts)
                return TimeSetDF(utils.merge_intervals(self.df.append(ts.df, ignore_index=True)))
            else:
                return self.copy()
        else:
            raise UnrecognizedTimeSet('right operand')

    def __sub__(self, ts):
        if isinstance(ts, API.TimeSet):
            if bool(self) and bool(ts):
                if not isinstance(ts, TimeSetDF):
                    try:
                        return ts.__rsub__(self)
                    except (AttributeError, NotImplementedError):
                        ts = TimeSetDF(ts)
                return TimeSetDF(utils.difference_b(self.df, ts.df))
        else:
            raise UnrecognizedTimeSet('right operand')
        return self.copy()

    def __iter__(self):
        return self.df.itertuples(index=False, name=None)

    def issuperset(self, ts):
        if not isinstance(ts, TimeSetDF):
            ts = TimeSetDF(ts)
        return utils.issuper_b(self.df, ts.df)
