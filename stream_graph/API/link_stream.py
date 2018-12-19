import copy
import abc
from stream_graph.stream_graph import StreamGraph

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

    @property
    @abc.abstractmethod
    def basic_nodestream(self):
        pass

    @property
    @abc.abstractmethod    
    def minimal_nodestream(self):
        pass

    @property
    @abc.abstractmethod
    def linkset(self):
        pass

    @property
    @abc.abstractmethod
    def timeset(self):
        pass

    @property
    @abc.abstractmethod
    def nodeset(self):
        pass

    @abc.abstractmethod
    def times_of(self, u, v, direction='out'):
        pass
    
    @abc.abstractmethod
    def neighbors_at(self, u, t, direction='out'):
        pass

    @abc.abstractmethod
    def neighbors(self, u, direction='out'):
        pass

    @abc.abstractmethod
    def substream(self, nsu, nsv, ts):
        pass

    @abc.abstractmethod
    def __bool__(self):
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
    def __iter__(self):
        pass

    @abc.abstractmethod
    def issuperset(self, ls):
        pass

    @abc.abstractmethod
    def induced_substream(self, ns):
        pass

    @abc.abstractmethod
    def neighborhood(self, ns, direction='out'):
        pass

    @property
    def m(self):
        return self.linkset.size

    def copy(self, deep=True):
        if deep:
            return copy.deepcopy(self)
        else:
            return copy.copy(self)

    def m_at(self, t):
        return self.links_at(t).size

    def link_duration(self, u, v, direction='out'):
        return self.times_of(u, v, direction=direction).size

    @property
    def as_stream_graph_basic(self):
        return StreamGraph(self.nodeset, self.timeset, self.basic_nodestream, self)

    @property
    def as_stream_graph_minimal(self):
        return StreamGraph(self.nodeset, self.timeset, self.basic_nodestream, self)
