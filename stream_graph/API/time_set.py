
import copy
import abc

class TimeSet(abc.ABC):
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
            copy.deepcopy(self)
        else:
            copy.copy(self)
