import copy
import abc

class LinkStream(abc.ABC):
    @property
    @abc.abstractmethod
    def size(self):
        pass

    @abc.abstractmethod
    def __contains__(self, l):
        pass

    @abc.abstractmethod
    def links_at(t):
        pass

    @abc.abstractmethod
    def times_of(self, u, v, direction='out'):
        pass
    
    @abc.abstractmethod
    def neighbors_at(self, u, t, direction='out'):
        pass
    
    @abc.abstractmethod
    def number_of_links_at(self, t):
        pass
    
    @abc.abstractmethod
    def neighbors(self, u, direction='out'):
        pass

    @abc.abstractmethod
    def substream(self, nsu, nsv, ts):
        pass

    @abc.abstractmethod
    def __and__(self, ls):
        pass

    @abc.abstractmethod
    def __or__(self, ls):
        pass

    @abc.abstractmethod
    def __sub__(self, ls):
        pass

    @abc.abstractmethod
    def issuperset(self, ls):
        pass
    
    def copy(self, deep=True):
        if deep:
            copy.deepcopy(self)
        else:
            copy.copy(self)
