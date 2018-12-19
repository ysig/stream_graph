import abc
import copy

class NodeStream(abc.ABC):
    @property
    @abc.abstractmethod
    def nodeset(self):
        pass

    @property
    @abc.abstractmethod
    def timeset(self):
        pass

    @property
    @abc.abstractmethod
    def size(self):
        pass

    @property
    @abc.abstractmethod
    def total_common_time(self):
        pass

    @property
    def n(self):
        return self.nodeset.size

    @property
    def total_time(self):
        return self.timeset.size

    @abc.abstractmethod
    def __contains__(self, u):
        pass

    def node_duration(self, u):
        return self.times_of(u).size

    @abc.abstractmethod
    def common_time(self, u, v=None):
        pass

    @abc.abstractmethod
    def nodes_at(self, t):
        pass

    @abc.abstractmethod
    def times_of(self, u):
        pass

    def n_at(self, t):
        return self.nodes_at(t).size

    @abc.abstractmethod
    def issuperset(self, ns):
        pass

    @abc.abstractmethod
    def __iter__(self):
        pass

    @abc.abstractmethod
    def __bool__(self):
        pass

    @abc.abstractmethod
    def __and__(self, ns):
        pass

    @abc.abstractmethod
    def __sub__(self, ns):
        pass

    @abc.abstractmethod
    def __or__(self, ns):
        pass

    def copy(self, deep=True):
        if deep:
            return copy.deepcopy(self)
        else:
            return copy.copy(self)
