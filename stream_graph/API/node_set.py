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
    
    def copy(self, deep=True):
        if deep:
            copy.deepcopy(self)
        else:
            copy.copy(self)
