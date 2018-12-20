from itertools import chain

from . import API
from .set.node_set_s import NodeSetS
from .df.time_set_df import TimeSetDF
from .df.node_stream_df import NodeStreamDF
from .exceptions import UnrecognizedNodeSet
from .exceptions import UnrecognizedTimeSet
from .exceptions import UnrecognizedNodeStream


class NodeStreamB(API.NodeStream):
    def __init__(self, nodeset=None, timeset=None):
        is_none = [a is not None for a in [timeset, nodeset]]
        if all(is_none):
            if isinstance(nodeset, API.NodeSet):
                self.nodeset_ = nodeset
            else:
                self.nodeset_ = NodeSetS(nodeset)

            if isinstance(timeset, API.TimeSet):
                self.timeset_ = timeset
            else:
                self.timeset_ = TimeSetDF(timeset)
                
        elif any(is_none):
            raise ValueError('All arguments should have values or be None')

    @property
    def nodeset(self):
        if hasattr(self, 'nodeset_'):
            return self.nodeset_.copy()
        else:
            return NodeSetS()

    @property
    def timeset(self):
        if hasattr(self, 'timeset_'):
            return self.timeset_.copy()
        else:
            return TimeSetDF()

    @property
    def size(self):
        if bool(self):
            return self.timeset_.size * self.n
        else:
            return 0.

    @property
    def n(self):
        if bool(self):
            return self.nodeset_.size
        else:
            return 0

    @property
    def total_time(self):
        if bool(self):
            return self.nodeset_.size
        else:
            return self.timeset_.size


    @property
    def total_common_time(self):
        n = self.n
        return n*(n-1)*self.timeset_.size


    def __contains__(self, u):
        assert type(u) is tuple and len(u) is 2
        if (not bool(self)) or (u[0] is None and u[1] is None):
            return False
        return ((u[0] is None or u[0] in self.nodeset_) and
                (u[1] is None or u[1] in self.timeset_))

    def node_duration(self, u):
        if u in self.nodeset_:
            return self.total_time
        else:
            return 0.

    def common_time(self, u, v=None):
        if bool(self) and u in self.nodeset_:
            if v is None:
                return (self.n-1) * self.total_time
            elif v in self.nodeset_:
                return self.total_time
        return 0.

    def issuperset(self, ns):
        if isinstance(ns, API.NodeStream):
            if bool(self) or bool(ns):
                if isinstance(ns, NodeStreamB):
                    return (self.timeset_.issuperset(ns.timeset_) and self.nodeset_.issuperset(ns.nodeset_))
                else:
                    return NodeStreamDF(self).issuperset(ns)
            else:
                return True
        else:
            raise UnrecognizedNodeStream('ns')
        return False

    def nodes_at(self, t):
        if bool(self) and (t in self.timeset_):
            return self.nodeset
        else:
            return NodeSetS()

    def n_at(self, t):
        if bool(self) and (t in self.timeset_):
            return self.n
        else:
            return 0

    def times_of(self, u):
        if bool(self) and u in self.timeset_:
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
                    return NodeStreamB(timeset=self.timeset_ & ns.timeset_, nodeset=self.nodeset_ & ns.nodeset_)
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
                    return NodeStreamB(timeset=self.timeset_ | ns.timeset_, nodeset=self.nodeset_ | ns.nodeset_)
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
                        ns_in = self.nodeset_ & ns.nodeset_
                        if bool(ns_in):
                            ts_diff = self.timeset_ - ns.timeset_
                            if bool(ts_diff):
                                return NodeStreamDF(chain(
                                    iter(NodeStreamB(
                                        nodeset=self.nodeset_ - ns.nodeset_,
                                        timeset=self.timeset_)),
                                    iter(NodeStreamB(
                                        nodeset=ns_in,
                                        timeset=ts_diff))))
                        ns_diff = self.nodeset_ - ns.nodeset_
                        if bool(ns_diff):
                            return NodeStreamB(nodeset=ns_diff, timeset=self.timeset_)
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
