from numbers import Real

import pandas as pd

from stream_graph import ABC
from stream_graph.exceptions import UnrecognizedTimeSet
from .utils import make_discrete_bins

class ITimeSetS(ABC.ITimeSet):
    """Set implementation of the ABC.ITimeSet"""
    def __init__(self, times=None, discrete=None):
        """Initialize a nodeset.
        
        Parameters
        ----------
        times: Iterable, default=None
            The Iterable should contain a valid Time (Int or Real).

        """
        if times is not None:
            self.times_ = set(t for t in times)
            if isinstance(times, ABC.ITimeSet):
                discrete = times.discrete
            elif discrete is None:
                discrete = False
            self.discrete_ = discrete

    @property
    def discrete(self):
        return (self.discrete_ if bool(self) else None)

    @property
    def size(self):
        if bool(self) and self.discrete:
            return len(self.times_)
        else:
            return 0

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
                assert self.discrete == ts.discrete
                if not isinstance(ts, self.__class__):
                    try:
                        return ts & self
                    except NotImplementedError:
                        ts = self.__class__(ts, discrete=self.discrete)
                return self.__class__(self.times_ & ts.times_, discrete=self.discrete)
        elif isinstance(ts, ABC.TimeSet):
            times = list(self.times_)
            assert self.discrete == ts.discrete
            return self.__class__((a for a, _ in (self.timeset_df & ts)), discrete=self.discrete)
        else:
            raise UnrecognizedTimeSet('right operand')
        return TimeSetDF()

    def __or__(self, ts):
        if isinstance(ts, ABC.ITimeSet):
            if not bool(self):
                return ts.copy()
            if bool(ts):
                assert self.discrete == ts.discrete
                if not isinstance(ts, ITimeSetS):
                    try:
                        return ts | self
                    except NotImplementedError:
                        ts = self.__class__(ts, discrete=self.discrete)
                return self.__class__(self.times_ | ts.times_, discrete=self.discrete)
            else:
                return self.copy()
        elif isinstance(ts, ABC.TimeSet):
            times = list(self.times_)
            assert self.discrete == ts.discrete
            return self.__class__((a for a, _ in (self.timeset_df | ts)), discrete=self.discrete)
        else:
            raise UnrecognizedTimeSet('right operand')

    def __sub__(self, ts):
        if isinstance(ts, ABC.ITimeSet):
            if bool(self) and bool(ts):
                assert self.discrete == ts.discrete
                if not isinstance(ts, ITimeSetS):
                    try:
                        return ts.__rsub__(self)
                    except (AttributeError, NotImplementedError):
                        ts = self.__class__(ts, discrete=self.discrete)
                return self.__class__(self.times_ - ts.times_, discrete=self.discrete)
        elif isinstance(ts, ABC.TimeSet):
            times = list(self.times_)
            assert self.discrete == ts.discrete
            return self.__class__((a for a, _ in (self.timeset_df - ts)), discrete=self.discrete)
        else:
            raise UnrecognizedTimeSet('right operand')
        return self.copy()

    def __iter__(self):
        return self.times_.__iter__()

    def issuperset(self, ts):
        if isinstance(ts, ABC.ITimeSet):
            assert self.discrete == ts.discrete
            if not isinstance(ts, ITimeSetS):
                ts = self.__class__(ts, discrete=self.discrete)
            return self.times_.issuperset(ts.times_)
        elif isinstance(ts, ABC.TimeSet):
            assert self.discrete == ts.discrete
            return self.timeset_df.issuperset(ts)
        else:
            raise UnrecognizedTimeSet('right operand')

    @property
    def timeset_df(self):
        times = list(self.times_)
        return TimeSetDF(pd.DataFrame({'ts': times, 'tf': times}), discrete=self.discrete)

    def _to_discrete(self, bins, bin_size):
        bins = make_discrete_bins(bins, bin_size, min(self.times_), max(self.times_))
        bin_map = {b: i for i, b in enumerate(bins)}
        return self.__class__((bin_map[t] for t in self), discrete=True), bins
