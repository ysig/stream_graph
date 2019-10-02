from __future__ import absolute_import
from numbers import Real

import pandas as pd

from stream_graph import ABC
from stream_graph.exceptions import UnrecognizedTimeSet
from .utils import make_discrete_bins


class ITimeSetS(ABC.ITimeSet):
    """Set implementation of the ABC.ITimeSet

    Parameters
    ----------
    times: Iterable, default=None
        The Iterable should contain a valid Time (Int or Real).

    """
    def __init__(self, times=None, discrete=None):
        if times is not None:
            self.times_ = set(t for t in times)
            if isinstance(times, ABC.ITimeSet):
                discrete = times.discrete
            elif discrete is None:
                # default assign
                if all(isinstance(t, int) for t in self.times_):
                    # Check if time signature can be considered discrete
                    discrete = True
                else:
                    discrete = False
            self.discrete_ = discrete

    @property
    def times(self):
        if hasattr(self, 'times_'):
            return self.times_.copy()
        else:
            return set()

    @property
    def discrete(self):
        return (self.discrete_ if bool(self) else None)

    @property
    def number_of_instants(self):
        return len(self.times_)

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
            # If instantaneous
            if bool(self) and bool(ts):
                assert self.discrete == ts.discrete
                if not isinstance(ts, self.__class__):
                    # If not of the same class
                    try:
                        # check if the other class has a method for it
                        return ts & self
                    except NotImplementedError:
                        # Else cast to class (through iterator)
                        ts = self.__class__(ts, discrete=self.discrete)
                return self.__class__(self.times_ & ts.times_, discrete=self.discrete)
        elif isinstance(ts, ABC.TimeSet):
            assert self.discrete == ts.discrete
            # Cast to TimeSetDF, apply the operation and keep only the instants.
            return self.__class__((a for a, _ in (self.timeset_df & ts)), discrete=self.discrete)
        else:
            raise UnrecognizedTimeSet('right operand')
        return self.__class__(discrete=self.discrete)

    def __or__(self, ts):
        if isinstance(ts, ABC.ITimeSet):
            # If instantaneous
            if not bool(self):
                return ts.copy()
            if bool(ts):
                assert self.discrete == ts.discrete
                if not isinstance(ts, self.__class__):
                    # If not of the same class
                    try:
                        # check if the other class has a method for it
                        return ts | self
                    except NotImplementedError:
                        # Else cast to class (through iterator)
                        ts = self.__class__(ts, discrete=self.discrete)
                return self.__class__(self.times_ | ts.times_, discrete=self.discrete)
            else:
                return self.copy()
        elif isinstance(ts, ABC.TimeSet):
            assert self.discrete == ts.discrete
            # If our input is not instantaneous, the result will probably be not instantaneous
            uni = self.timeset_df | ts
            if any(a != b for a, b in uni):
                # If so return output as is
                return uni
            else:
                # Else cast to the instantaneous class of `self`
                return self.__class__((a for a, _ in uni), discrete=self.discrete)
        else:
            raise UnrecognizedTimeSet('right operand')

    def __sub__(self, ts):
        if isinstance(ts, ABC.ITimeSet):
            if bool(self) and bool(ts):
                assert self.discrete == ts.discrete
                if not isinstance(ts, ITimeSetS):
                    try:
                        # check if the other class has a method for it
                        # [notice that we search for `__rsub__`]
                        return ts.__rsub__(self)
                    except (AttributeError, NotImplementedError):
                        # Else cast to class (through iterator)
                        ts = self.__class__(ts, discrete=self.discrete)
                return self.__class__(self.times_ - ts.times_, discrete=self.discrete)
        elif isinstance(ts, ABC.TimeSet):
            assert self.discrete == ts.discrete
            # Cast to TimeSetDF subtract and then keep the instantaneous part, to get a valid output
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
        from stream_graph import TimeSetDF
        times = list(self.times_)
        # Cast to a TimeSetDF with the same start and finish
        return TimeSetDF(pd.DataFrame({'ts': times, 'tf': times}), discrete=self.discrete)

    def _to_discrete(self, bins, bin_size):
        bins = make_discrete_bins(bins, bin_size, min(self.times_), max(self.times_))
        bin_map = {b: i for i, b in enumerate(bins)}
        return self.__class__((bin_map[t] for t in self), discrete=True), bins
