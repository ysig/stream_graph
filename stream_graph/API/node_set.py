import copy
import abc

class NodeSet(abc.ABC):
    @property
    @abc.abstractmethod
    def size(self):
        pass
    
    @abc.abstractmethod
    def __contains__(self, n):
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
    def issuperset(self):
        pass
    
    def copy(self, deep=True):
        if deep:
            return copy.deepcopy(self)
        else:
            return copy.copy(self)
