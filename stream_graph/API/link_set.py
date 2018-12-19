import copy
import abc

class LinkSet(abc.ABC):
    @property
    @abc.abstractmethod
    def size(self):
        pass

    @abc.abstractmethod
    def neighbors(self, u, direction='out'):
        pass

    @abc.abstractmethod
    def degree(self, u, direction='out'):
        pass

    @abc.abstractmethod
    def __contains__(self, l):
        pass

    @abc.abstractmethod
    def __and__(self, ns):
        pass

    @abc.abstractmethod
    def __or__(self, ns):
        pass

    @abc.abstractmethod
    def __sub__(self, ns):
        pass

    @abc.abstractmethod
    def __iter__(self):
        pass

    @abc.abstractmethod
    def issuperset(self, ls):
        pass

    def copy(self, deep=True):
        if deep:
            return copy.deepcopy(self)
        else:
            return copy.copy(self)
