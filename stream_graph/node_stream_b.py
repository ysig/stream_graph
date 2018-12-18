from stream_graph import API
from stream_graph.set.node_set_s import NodeSetS
from stream_graph.df.time_set_df import TimeSetDF
from stream_graph.df.node_stream_df import NodeStreamDF
from stream_graph.exceptions import UnrecognizedNodeSet
from stream_graph.exceptions import UnrecognizedTimeSet
from stream_graph.exceptions import UnrecognizedNodeStream


class NodeStreamB(API.NodeStream):
    def __init__(self, timeset=None, nodeset=None):
        is_none = [a is not None for a in [timeset, nodeset]]
        if all(is_none):
            if isinstance(API.TimeSet, timeset):
                self.timeset_ = timeset
            else:
                raise UnrecognizedTimeSet('timeset')
            if isinstance(API.NodeSet, nodeset):
                self.nodeset_ = nodeset
            else:
                raise UnrecognizedNodeSet('nodeset')
        elif any(is_none):
            raise ValueError('All arguments should have values or be None')

    @property
    def ns(self):
        return self.nodeset_

    @property
    def ts(self):
        return self.timeset_

    @property
    def nodeset(self):
        return self.nodeset_.copy()

    @property
    def timeset(self):
        return self.timeset_.copy()

    @property
    def size(self):
        return self.timeset_.size * self.n

    @property
    def n(self):
        return self.nodeset_.size

    @property
    def total_time(self):
        return self.timeset_.size

    @property
    def total_common_time(self):
        n = self.n
        return n*(n-1)*self.timeset_.total_time


    def __contains__(self, u):
        assert type(u) is tuple and len(u) is 2
        if (not bool(self)) or (u[0] is None and u[1]):
            return False
        return ((u[0] is None or u[0] in self.ns) and
                (u[1] is None or u[1] in self.ts))

    def node_duration(self, u):
        if u in self.ns:
            return self.total_time
        else:
            return 0.

    def common_time(self, u, v=None):
        if bool(self) and u in self.ns:
            if v is None:
                return (self.n-1) * self.total_time
            elif v in self.ns:
                return self.total_time
        return 0.

    def number_of_nodes_at(self, t):
        return self.n

    def issuperset(self, ns):
        if isinstance(ns, API.NodeStream):
            if bool(ns):
                return (self.ts.issuperset(ns.ts) and self.ns.issuperset(ns.ns))
            else:
                return True
        else:
            raise UnrecognizedNodeStream('ns')
        return False

    def nodes_at(self, t):
        if bool(self) and t in self.ts:
            return self.nodeset
        else:
            return NodeSetS()

    def times_of(self, u):
        if bool(self) and u in self.ts:
            return self.timeset
        else:
            return TimeSetDF()

    def __iter__(self):
        for a in self.nodeset_:
            for b, c in self.timeset_:
                yield (a, b, c)

    def __bool__(self):
        return hasattr(self, 'nodeset_') and hasattr(self, 'timeset_') and bool(self.nodeset_) and bool(self.timeset_)

    def __and__(self, ns):
        if isinstance(ns, API.NodeStream):
            if isinstance(ns, NodeStreamB):
                if ns and bool(self):
                    return NodeStreamB(timeset=self.ts & ns.ts, nodeset=self.ns & ns.ns)
            else:
                return ns & self
        else:
            raise UnrecognizedNodeStream('right operand')
        return NodeStreamB()

    def __or__(self, ns):
        if isinstance(ns, API.NodeStream):
            if not bool(self):
                return ns.copy()
            if isinstance(ns, NodeStreamB):
                if bool(ns):
                    return NodeStreamB(timeset=self.ts | ns.ts, nodeset=self.ns | ns.ns)
                else:
                    return self.copy()
            else:
                return ns | self
        else:
            raise UnrecognizedNodeStream('right operand')
        return NodeStreamB()

    def __sub__(self, ns):
        if isinstance(ns, API.NodeStream):
            if bool(self):
                if bool(ns):
                    if isinstance(ns, NodeStreamB):
                        return NodeStreamB(timeset=self.ts - ns.ts, nodeset=self.ns - ns.ns)
                    else:
                        try:
                            return ns.__rsub__(self)
                        except (AttributeError, NotImplementedError):
                            return NodeStreamDF(self) - NodeStreamDF(ns)
            else:
                return self.copy()
        else:
            raise UnrecognizedNodeStream('right operand')
        return NodeStreamB()
