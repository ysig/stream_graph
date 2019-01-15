from . import ABC
from .exceptions import UnrecognizedStreamGraph

class StreamGraph(object):
    def __init__(self, nodeset=None, timeset=None, nodestream=None, linkstream=None):
        if not isinstance(nodeset, ABC.NodeSet):
            from . import NodeSetS
            self.nodeset_ = NodeSetS(nodeset)
        else:
            self.nodeset_ = nodeset
        if not isinstance(timeset, ABC.TimeSet):
            from . import TimeSetDF
            self.timeset_ = TimeSetDF(timeset)
        else:
            self.timeset_ = timeset
        if not isinstance(nodestream, ABC.NodeStream):
            from . import NodeStreamDF
            self.nodestream_ = NodeStreamDF(nodestream)
        else:
            self.nodestream_ = nodestream
        if not isinstance(linkstream, ABC.LinkStream):
            from . import LinkStreamDF
            self.linkstream_ = LinkStreamDF(linkstream)
        else:
            self.linkstream_ = linkstream

    def __bool__(self):
        return ((hasattr(self, 'nodeset_') and bool(self.nodeset_)) and
                (hasattr(self, 'timeset_') and bool(self.timeset_)) and
                (hasattr(self, 'nodestream_') and bool(self.nodestream_)) and
                (hasattr(self, 'linkstream_') and bool(self.linkstream_)))

    # Python2 cross-compatibility
    __nonzero__ = __bool__

    @property
    def nodeset(self):
        return self.nodeset_.copy()

    @property
    def timeset(self):
        return self.timeset_.copy()

    @property
    def nodestream(self):
        return self.nodestream_.copy()

    @property
    def linkstream(self):
        return self.linkstream_.copy()

    @property
    def empty(self):
        return not bool(self)

    def graph_at(t):
        return Graph(self.nodestream_.nodes_at(t), self.linkstream_.links_at(t))

    @property
    def linkstream_coverage(self):
        denom = float(self.nodeset_.size * self.nodestream_.total_common_time)
        if denom > .0:
            return self.linkstream_.size / denom
        else:
            return .0

    @property
    def nodestream_coverage(self):
        denom = float(self.nodeset_.size * self.nodestream_.total_common_time)
        if denom > .0:
            return self.linkstream_.size / denom
        else:
            return .0

    def time_coverage(self, u, v=None, direction='out'):
        denom = float(self.timeset_.size)
        if denom == .0:
            return .0
        elif v is None:
            return self.nodestream_.times_of(u).size / denom
        else:
            return self.linkstream_.times_of(u, v, direction).size / denom

    def node_coverage(self, t):
        denom = float(self.nodeset_.size )
        if denom > .0:
            return self.nodestream_.n_at(t) / denom
        else:
            return .0

    def link_coverage(self, t):
        denom = float(self.nodestream_.nodes_at(t).size ** 2)
        if denom > .0:
            return self.linkstream_.links_at(t).size / denom
        else:
            return .0

    def neighbor_coverage(self, u, direction='out'):
        denom = float(self.nodeset_.size * self.timeset_.size)
        if denom > .0:
            return self.linkstream_.neighbors(u, direction).size / denom
        else:
            return .0

    def neighbor_coverage_at(self, u, t, direction='out'):
        denom = float(self.nodeset_.size)
        if denom > .0:
            return self.linkstream_.neighbors_at(u, t, direction).size / denom
        else:
            return .0

    @property
    def total_density(self):
        denom = float(self.nodestream_.total_common_time)
        if denom > .0:
            return self.linkstream_.size / denom
        else:
            return .0

    def density(self, u, v=None, direction='out'):
        denom = float(self.nodestream_.common_time(u, v))
        if denom == .0:
            return .0
        elif v is None:
            return self.linkstream_.neighbors(u, direction).size / denom
        else:
            return self.linkstream_.times_of(u, v, direction).size / denom

    def density_at(self, t):
        denom = float(self.nodestream_.n_at(t))
        if denom > .0:
            return self.linkstream_.linkstream_at(t).size / denom
        else:
            return .0

    def __and__(self, sg):
        if isinstance(sg, StreamGraph):
            return StreamGraph(self.nodeset_ & sg.nodeset_,
                               self.timeset_ & sg.timeset_,
                               self.nodestream_ & sg.nodestream_,
                               self.linkstream_ & sg.linkstream_)
        else:
            raise UnrecognizedStreamGraph('right operand')

    def __or__(self, sg):
        if isinstance(sg, StreamGraph):
            return StreamGraph(self.nodeset_ | sg.nodeset_,
                               self.timeset_ | sg.timeset_,
                               self.nodestream_ | sg.nodestream_,
                               self.linkstream_ | sg.linkstream_)
        else:
            raise UnrecognizedStreamGraph('right operand')

    def __sub__(self, sg):
        if isinstance(sg, StreamGraph):
            nsm = self.nodestream_ - sg.nodestream_
            return StreamGraph((self.nodeset_ - sg.nodeset_) | nsm.nodeset,
                               self.timeset_ - sg.timeset_, nsm,
                               self.linkstream_ - sg.linkstream_)
        else:
            raise UnrecognizedStreamGraph('right operand')

    def issuperset(self, sg):
        if isinstance(sg, StreamGraph):
            return (self.nodeset_.issuperset(sg.nodeset_) and
                    self.timeset_.issuperset(sg.timeset_) and
                    self.nodestream_.issuperset(sg.nodestream_) and
                    self.linkstream_.issuperset(sg.linkstream_))
        else:
            raise UnrecognizedStreamGraph('right operand')
        return False

    @property
    def n(self):
        denom = float(self.timeset_.size)
        if denom > .0:
            return self.nodestream_.size/denom
        else:
            return .0

    @property
    def m(self):
        denom = float(self.timeset_.size)
        if denom > .0:
            return self.linkstream_.size/denom
        else:
            return .0

    def contribution(self, u, v=None, direction='out'):
        denom = float(self.timeset_.size)
        if denom > .0:
            if v is None:
                return self.nodestream_.node_duration(u) / denom
            else:
                return self.linkstream_.link_duration(u, v, direction=direction) / denom
        else:
            return .0

    def induced_substream(self, ns):
        assert isinstance(ns, NodeStream)
        ns_is = self.ns_ & ns
        ls_ind = self.linkstream_.induced_substream(ns_is)
        return StreamGraph(self.nodeset_.copy(), self.timeset_.copy(), ns_is, ls_ind)
