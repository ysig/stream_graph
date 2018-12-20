import copy
import abc

# 2/3 Cross Compatibility
ABC = abc.ABCMeta('ABC', (object,), {'__slots__': ()}) 

class TimeSet(ABC):
    @property
    @abc.abstractmethod
    def size(self):
        pass

    @abc.abstractmethod
    def __contains__(self, t):
        pass

    @abc.abstractmethod
    def __and__(self, ts):
        pass

    @abc.abstractmethod
    def __or__(self, ts):
        pass

    @abc.abstractmethod
    def __sub__(self, ts):
        pass

    @abc.abstractmethod
    def __iter__(self):
        pass

    def copy(self, deep=True):
        if deep:
            return copy.deepcopy(self)
        else:
            return copy.copy(self)
