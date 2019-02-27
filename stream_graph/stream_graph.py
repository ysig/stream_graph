from . import ABC
from six import iteritems
from .exceptions import UnrecognizedStreamGraph

class StreamGraph(object):
    """StreamGraph
    
    A StreamGraph :math:`S=(T, V, W, E)` is a collection of four elements:
    - :math:`T`, a time-set
    - :math:`V`, a node-set
    - :math:`W\subseteq T \times V`, a node-stream
    - :math:`E\subseteq T \times V \otimes V`, a link-stream

    """
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
        """Calculate the coverage of the link-stream.
        
        Parameters
        ----------
        None. Property.
        
        Returns
        -------
        ns_coverage : Real
            Returns :math:`\frac{|E|}{\sum_{uv \in V\times V}|T_{u} \cap T_{v}|}`

        """
        denom = float(self.nodestream_.total_common_time)
        if denom > .0:
            return 2*self.linkstream_.size / denom
        else:
            return .0

    @property
    def nodestream_coverage(self):
        """Calculate the coverage of the node-stream.
        
        Parameters
        ----------
        None. Property.
        
        Returns
        -------
        ns_coverage : Real
            Returns :math:`\frac{|W|}{|V\timesT|}}`

        """
        denom = float(self.timeset_.size * self.nodeset_.size)
        if denom > .0:
            return self.nodestream_.size / denom
        else:
            return .0

    def time_coverage_node(self, u=None, direction='out'):
        """Calculate the time coverage of a node inside the stream_graph.
        
        Parameters
        ----------
        u: NodeId or None
        
        direction: 'in', 'out' or 'both', default='out'
        
        Returns
        -------
        time_coverage_node : Real or dict
            Returns :math:`\frac{|T_{u}|}{|T|}`.
            If u is None, returns a dictionary of all nodes and their coverages.

        """
        denom = float(self.timeset_.size)
        if denom == .0:
            if u is None:
                return {u: .0 for u in self.nodeset_}
            else:
                return .0
        else:
            if u is None:
                times = self.nodestream_.times_of()
                return {u: ts.size/denom for u, ts in iteritems(times)}
            else:
                return self.nodestream_.times_of(u).size / denom

    def time_coverage_link(self, l=None, direction='out'):
        """Calculate the time coverage of a link inside the stream_graph.
        
        Parameters
        ----------
        l: (NodeId, NodeId) or None
        
        direction: 'in', 'out' or 'both', default='out'
        
        Returns
        -------
        time_coverage : Real or dict
            Returns :math:`\frac{|T_{uv}|}{|T_{u} \cap T_{v}|}`.
            If l is None, returns a dictionary of all links and their coverages.

        """
        if l is None:
            times = self.linkstream_.times_of(direction=direction)
            active_links = set(k for k, v in iteritems(times) if v > .0)
            if len(active_links):
                common_times = self.nodestream_.common_times_pair(l=links)
                time_coverage_links = dict()
                for k, v in iteritems(times):
                    if v > .0:
                        ct = common_times[k]
                        if ct > .0:
                            time_coverage_links[k] = v/float(ct)
                            continue
                    time_coverage_links[k] = .0 
                return time_coverage_links
            else:
                return {l: .0 for k in active_links.keys()}
        else:
            denom = float(self.nodestream_.common_times_pair(l)
            if denom == .0:
                return .0
            else:
                return self.linkstream_.times_of(l, direction).size / denom

    def node_coverage_at(self, t):
        """Calculate the node coverage of a time instant inside the stream_graph.
        
        Parameters
        ----------
        t: time
        
        Returns
        -------
        node_coverage : Real
            Returns :math:`\frac{|V_{t}|}{|V|}`.

        """
        denom = float(self.nodeset_.size )
        if denom > .0:
            return self.nodestream_.n_at(t) / denom
        else:
            return .0

    def link_coverage_at(self, t):
        """Calculate the link coverage of a time instant inside the stream_graph.
        
        Parameters
        ----------
        t: time
        
        Returns
        -------
        link_coverage : Real
            Returns :math:`\frac{|E_{t}|}{|V|*|V|}`

        """
        denom = float(self.nodestream_.nodes_at(t).size ** 2)
        if denom > .0:
            return 2 * self.linkstream_.links_at(t).size / denom
        else:
            return .0

    def neighbor_coverage(self, u=None, direction='out'):
        """Calculate the neighbor coverage of a node inside the stream_graph.
        
        Parameters
        ----------
        u: NodeId or None
        
        direction: 'in', 'out' or 'both', default='out'
        
        Returns
        -------
        neighbor_coverage : Real or dict
            Returns :math:`\frac{|N(u)|}{\sum_{v\in V}|T_{u}\cap T_{v}|}`
            If u is None, returns a dictionary of all nodes and their neighbor coverages.

        """
        if l is None:
            times = {k: v.size for k, v in iteritems(self.linkstream_.neighbors(direction=direction))}
            common_times = self.nodestream_.common_times()# maybe add a u = nodes argument in nodestream_common_times
            neighbor_coverage = dict()
            for k, v in iteritems(times):
                if v > .0:
                    ct = common_times[k]
                    if ct > .0:
                        neighbor_coverage[k] = v/float(ct)
                        continue
                neighbor_coverage[k] = .0 
            return neighbor_coverage
        else:
            denom = float(self.nodestream_.common_times_pair(l)
            if denom == .0:
                return .0
            else:
                return self.linkstream_.neighbors(l, direction).size / denom

    def neighbor_coverage_at(self, u, t, direction='out'):
        """Calculate the coverage of a node inside the stream_graph.
        
        Parameters
        ----------
        u: NodeId or None

        t: time

        direction: 'in', 'out' or 'both', default='out'
        
        Returns
        -------
        time_coverage : Real
            Returns :math:`\frac{|N_{t}_(u)|}{|V|}`

        """
        denom = float(self.nodestream_.n_at(t))
        if denom > .0:
            return self.linkstream_.neighbors_at(u, t, direction).size / denom
        else:
            return .0

    def mean_degree_at(self, t):
        """Calculate the mean degree at a give time.
        
        Parameters
        ----------
        t: Time
        
        Returns
        -------
        time_coverage : Real
            Returns :math:`\frac{|E_{t}|}{|W_{t}|}`

        """
        denom = float(self.nodestream_.n_at(t))
        if denom > .0:
            return self.linkstream_.m_at(t) / denom
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
        """Calculate the number of nodes of the stream-graph.
        
        Parameters
        ----------
        None. Property.
        
        Returns
        -------
        n : Real
            Returns :math:`\frac{|W|}{|T|}`

        """
        denom = float(self.timeset_.size)
        if denom > .0:
            return self.nodestream_.size/denom
        else:
            return .0

    @property
    def m(self):
        """Calculate the number of edges of the stream-graph.
        
        Parameters
        ----------
        None. Property.
        
        Returns
        -------
        n : Real
            Returns :math:`\frac{|E|}{|T|}`

        """
        denom = float(self.timeset_.size)
        if denom > .0:
            return self.linkstream_.size/denom
        else:
            return .0

    def induced_substream(self, ns):
        assert isinstance(ns, NodeStream)
        ns_is = self.ns_ & ns
        ls_ind = self.linkstream_.induced_substream(ns_is)
        return StreamGraph(self.nodeset_.copy(), self.timeset_.copy(), ns_is, ls_ind)
