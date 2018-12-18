from stream_graph import API
from stream_graph.exceptions import UnrecognizedNodeSet
from stream_graph.exceptions import UnrecognizedTimeSet
from stream_graph.exceptions import UnrecognizedNodeStream
from stream_graph.exceptions import UnrecognizedLinkStream


class StreamGraph(object):
    def __init__(self, nodeset, timeset, nodestream, linkstream):
        if not isinstance(nodeset, API.NodeSet):
            raise UnrecognizedNodeSet('nodeset')
        if not isinstance(nodeset, API.TimeSet):
            raise UnrecognizedNodeSet('timeset')
        if not isinstance(nodestream, API.NodeStream):
            raise UnrecognizedNodeSet('nodestream')
        if not isinstance(linkstream, API.LinkStream):
            raise UnrecognizedNodeSet('linkstream')
        self.nst_, self.tst_, self.nsm_, self.lsm_ = nodeset, timeset, nodestream, linkstream

    def __bool__(self):
        return ((hasattr(self, 'nst_') and bool(self.nst_)) and
                (hasattr(self, 'tst_') and bool(self.tst_)) and
                (hasattr(self, 'nsm_') and bool(self.nsm_)) and
                (hasattr(self, 'lsm_') and bool(self.lsm_)))

    @property
    def empty(self):
        return not bool(self)

    def graph_at(t):
        return Graph(self.nsm_.nodes_at(t), self.lsm_.links_at(t))

    @property
    def lsm_coverage(self):
        denom = float(self.nst_.size * self.nsm.total_common_time)
        if denom > .0:
            return self.lsm.size / denom
        else:
            return .0

    @property
    def nsm_coverage(self):
        denom = float(self.nst_.size * self.nsm.total_common_time)
        if denom > .0:
            return self.lsm.size / denom
        else:
            return .0

    def time_coverage(self, u, v=None, direction='out'):
        denom = float(self.ts.size)
        if denom == .0:
            return .0
        elif v is None:
            return self.nsm_.times_of(u).size / denom
        else:
            return self.lsm_.times_of(u, v, direction).size / denom

    def node_coverage(self, t):
        denom = float(self.nst_.size )
        if denom > .0:
            return self.nsm_.number_of_nodes_at(t) / denom
        else:
            return .0

    def link_coverage(self, t):
        denom = float(self.nsm_.nodes_at(t).size ** 2)
        if denom > .0:
            return self.lsm_.links_at(t).size / denom
        else:
            return .0

    def neighbor_coverage(self, u, direction='out'):
        denom = float(self.nst_.size * self.tst_.size)
        if denom > .0:
            return self.lsm_.neighbors(u, direction).size / denom
        else:
            return .0

    def neighbor_coverage_at(self, u, t, direction='out'):
        denom = float(self.nst_.size)
        if denom > .0:
            return self.lsm_.neighbors_at(u, t, direction) / denom
        else:
            return .0

    @property
    def total_density(self):
        denom = float(self.nsm_.total_common_time)
        if denom > .0:
            return self.lsm_.size / denom
        else:
            return .0

    def density(self, u, v=None, direction='out'):
        denom = float(self.nsm_.common_time(u, v))
        if denom == .0:
            return .0
        elif v is None:
            return self.lsm_.neighbors(u, direction).size / denom
        else:
            return self.lsm_.times_of(u, v, direction).size / denom

    def density_at(self, t):
        denom = float(self.nsm_.number_of_nodes_at(t))
        if denom > .0:
            return self.lsm_.linkstream_at(t).size / denom
        else:
            return .0

    def __and__(self, sg):
        if isinstance(sg, StreamGraph):
            return StreamGraph(self.nst_ & sg.nst_,
                               self.tst_ & sg.tst_,
                               self.nsm_ & sg.nsm_,
                               self.lsm_ & sg.lsm_)
        else:
            raise ValueError('Right Operand should must be a StreamGraph')

    def __or__(self, sg):
        if isinstance(sg, StreamGraph):
            return StreamGraph(self.nst_ | sg.nst_,
                               self.tst_ | sg.tst_,
                               self.nsm_ | sg.nsm_,
                               self.lsm_ | sg.lsm_)
        else:
            raise ValueError('Right Operand should must be a StreamGraph')

    def __sub__(self, sg):
        if isinstance(sg, StreamGraph):
            return StreamGraph(self.nst_ - sg.nst_,
                               self.tst_ - sg.tst_,
                               self.nsm_ - sg.nsm_,
                               self.lsm_ - sg.lsm_)
        else:
            raise ValueError('Right Operand should must be a StreamGraph')

    def issuperset(self, sg):
        if isinstance(sg, StreamGraph):
            return (self.nst_.issuperset(sg.nst_) and
                    self.tst_.issuperset(sg.tst_) and
                    self.nsm_.issuperset(sg.nsm_) and
                    self.lsm_.issuperset(sg.lsm_))
        else:
            raise ValueError('ns must be an object that implements NodeStream')
        return False

    @property
    def n(self):
        denom = float(self.tst_.size)
        if denom > .0:
            return self.nsm_.size/denom
        else:
            return .0

    @property
    def m(self):
        denom = float(self.tst_.size)
        if denom > .0:
            return self.lsm_.size/denom
        else:
            return .0

    def contribution(self, u, v=None, direction='out'):
        return self.link_duration(u, v=None, direction=direction)/float(self.ns_.total_time)

    def induced_substream(self, ns):
        assert isinstance(ns, NodeStream)
        ns_is = self.ns_ & ns
        ls_ind = self.lsm_.induced_substream(ns_is)
        return StreamGraph(self.nst_.copy(), self.tst_.copy(), ns_is, ls_ind)
