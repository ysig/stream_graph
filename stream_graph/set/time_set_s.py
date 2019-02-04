from numbers import Real

import pandas as pd

from stream_graph import ABC
from stream_graph.exceptions import UnrecognizedTimeSet


class ITimeSetS(ABC.ITimeSet):
    def __init__(self, times=None):
        if times is not None:
            self.times_ = set(times)

    def __bool__(self):
        return hasattr(self, 'times_') and (len(self.times_) > 0)

    def __contains__(self, t):
        if bool(self):
            if isinstance(t, Real):
                return t in self.times_
            else:
                raise ValueError('Input can either be a real number or an ascending interval of two real numbers')
        else:
            return False

    def __and__(self, ts):
        if isinstance(ts, ABC.ITimeSet):
            if bool(self) and bool(ts):
                if not isinstance(ts, ITimeSetS):
                    try:
                        return ts & self
                    except NotImplementedError:
                        ts = ITimeSetS(ts)
                return ITimeSetS(self.times_ & ts.times_)
        elif isinstance(ts, ABC.TimeSet):
            times = list(self.times_)
            return ITimeSetS(a for a, _ in (self.timeset_df & ts))
        else:
            raise UnrecognizedTimeSet('right operand')
        return TimeSetDF()

    def __or__(self, ts):
        if isinstance(ts, ABC.ITimeSet):
            if not bool(self):
                return ts.copy()
            if bool(ts):
                if not isinstance(ts, ITimeSetS):
                    try:
                        return ts | self
                    except NotImplementedError:
                        ts = ITimeSetS(ts)
                return ITimeSetS(self.times_ | ts.times_)
            else:
                return self.copy()
        elif isinstance(ts, ABC.TimeSet):
            times = list(self.times_)
            return ITimeSetS(a for a, _ in (self.timeset_df | ts))
        else:
            raise UnrecognizedTimeSet('right operand')

    def __sub__(self, ts):
        if isinstance(ts, ABC.ITimeSet):
            if bool(self) and bool(ts):
                if not isinstance(ts, ITimeSetS):
                    try:
                        return ts.__rsub__(self)
                    except (AttributeError, NotImplementedError):
                        ts = ITimeSetS(ts)
                return ITimeSetS(self.times_ - ts.times_)
        elif isinstance(ts, ABC.TimeSet):
            times = list(self.times_)
            return ITimeSetS(a for a, _ in (self.timeset_df - ts))
        else:
            raise UnrecognizedTimeSet('right operand')
        return self.copy()

    def __iter__(self):
        return self.times_.__iter__()

    def issuperset(self, ts):
        if isinstance(ts, ABC.ITimeSet):
            if not isinstance(ts, ITimeSetS):
                ts = ITimeSetS(ts)
            return self.times_.issuperset(ts.times_)
        elif isinstance(ts, ABC.TimeSet):
            return self.timeset_df.issuperset(ts)
        else:
            raise UnrecognizedTimeSet('right operand')

    @property
    def timeset_df(self):
        times = list(self.times_)
        return TimeSetDF(pd.DataFrame({'ts': times, 'tf': times}))
