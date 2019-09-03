from __future__ import absolute_import
from warnings import warn
from stream_graph import ABC
from stream_graph.exceptions import UnrecognizedStreamGraph
from stream_graph.collections import DataCube


class StreamGraph(object):
    """ StreamGraph

    A StreamGraph :math:`S=(T, V, W, E)` is a collection of four elements:

        - :math:`V`, a node-set
        - :math:`T`, a time-set
        - :math:`W\\subseteq T \\times V`, a temporal-node-set
        - :math:`E\\subseteq T \\times V \\otimes V`, a temporal-link-set


    Parameters:
    -----------
    nodeset: ABC.NodeSet

    timeset: ABC.TimeSet or ABC.ITimeSet

    temporal_nodeset: ABC.TemporalNodeSet or ABC.ITemporalNodeSet

    temporal_linkset: ABC.TemporalLinkSet or ABC.ITemporalLinkSet

    """
    def __init__(self, nodeset=None, timeset=None, temporal_nodeset=None, temporal_linkset=None, discrete=None, weighted=False):
        if not isinstance(nodeset, ABC.NodeSet):
            from . import NodeSetS
            self.nodeset_ = NodeSetS(nodeset)
        else:
            self.nodeset_ = nodeset
        if not isinstance(timeset, ABC.TimeSet):
            from .time_set_df import TimeSetDF
            if discrete is None:
                discrete = False
            self.timeset_ = TimeSetDF(timeset, discrete=discrete)
        else:
            self.timeset_ = timeset
            discrete = timeset.discrete
        if not isinstance(temporal_nodeset, ABC.TemporalNodeSet):
            from . import TemporalNodeSetDF
            self.temporal_nodeset_ = TemporalNodeSetDF(temporal_nodeset, discrete=self.timeset_.discrete)
        else:
            self.temporal_nodeset_ = temporal_nodeset
            assert self.timeset_.discrete is None or self.timeset_.discrete == self.temporal_nodeset_.discrete
        if not isinstance(temporal_linkset, ABC.TemporalLinkSet):
            from . import TemporalLinkSetDF
            self.temporal_linkset_ = TemporalLinkSetDF(temporal_linkset, discrete=self.timeset_.discrete, weighted=weighted)
        else:
            self.temporal_linkset_ = temporal_linkset
            assert self.timeset_.discrete is None or self.timeset_.discrete == self.temporal_linkset_.discrete

    def __bool__(self):
        return ((hasattr(self, 'nodeset_') and bool(self.nodeset_)) and
                (hasattr(self, 'timeset_') and bool(self.timeset_)) and
                (hasattr(self, 'temporal_nodeset_') and bool(self.temporal_nodeset_)) and
                (hasattr(self, 'temporal_linkset_') and bool(self.temporal_linkset_)))

    # Python2 cross-compatibility
    __nonzero__ = __bool__

    def __str__(self):
        if bool(self):
            out = [('Node-Set', str(self.nodeset_))]
            out += [('Time-Set', str(self.timeset_))]
            out += [('Temporal-Node-Set', str(self.temporal_nodeset_))]
            out += [('Temporal-Link-Set', str(self.temporal_linkset_))]
            header = ['Stream-Graph']
            header += [len(header[0])*'=']
            return '\n\n'.join(['\n'.join(header)] + ['\n'.join([a, len(a)*'-', b]) for a, b in out])
        else:
            out = ["Empty Stream-Graph"]
            out = [out[0] + "\n" + len(out[0])*'-']
            if not hasattr(self, 'nodeset_'):
                out += ['- Node-Set: None']
            elif not bool(self.nodeset_):
                out += ['- Node-Set: Empty']

            if not hasattr(self, 'timeset_'):
                out += ['- Time-Set: None']
            elif not bool(self.timeset_):
                out += ['- Time-Set: Empty']

            if not hasattr(self, 'temporal_nodeset_'):
                out += ['- Temporal-Node-Set: None']
            elif not bool(self.temporal_nodeset_):
                out += ['- Temporal-Node-Set: Empty']

            if not hasattr(self, 'temporal_linkset_'):
                out += ['- Temporal-Link-Set: None']
            elif not bool(self.temporal_linkset_):
                out += ['- Temporal-Link-Set: Empty']
            return '\n\n  '.join(out)

    @property
    def weighted(self):
        return self.temporal_linkset_.weighted

    @property
    def discrete(self):
        return self.timeset_.discrete

    @property
    def nodeset(self):
        return self.nodeset_.copy()

    @property
    def timeset(self):
        return self.timeset_.copy()

    @property
    def linkset(self):
        return self.temporal_linkset_.linkset    

    @property
    def temporal_nodeset(self):
        return self.temporal_nodeset_.copy()

    @property
    def temporal_linkset(self):
        return self.temporal_linkset_.copy()

    @property
    def empty(self):
        return not bool(self)

    def graph_at(t):
        if t is None:
            def fun(nodes, links):
                return Graph(nodes, links)
            return self.temporal_nodeset_.nodes_at(t).merge(self.temporal_linkset_.links_at(t), fun)
        else:
            return Graph(self.temporal_nodeset_.nodes_at(t), self.temporal_linkset_.links_at(t))

    @property
    def temporal_linkset_coverage(self):
        """Calculate the coverage of the temporal-link-set.

        Parameters
        ----------
        None. Property.

        Returns
        -------
        ns_coverage : Real
            Returns :math:`\\frac{|E|}{\sum_{uv \\in V\\times V}|T_{u} \\cap T_{v}|}`

        """
        denom = float(self.temporal_nodeset_.total_common_time)
        if denom > .0:
            return 2*self.temporal_linkset_.size / denom
        else:
            return .0

    @property
    def weighted_temporal_linkset_coverage(self):
        """Calculate the weighted coverage of the temporal-link-set.

        Parameters
        ----------
        None. Property.

        Returns
        -------
        ns_coverage : Real
            Returns :math:`\\frac{|E_{w}|}{\sum_{uv \\in V\\times V}|T_{u} \\cap T_{v}|}`

        """
        denom = float(self.temporal_nodeset_.total_common_time)
        if denom > .0:
            return 2*self.temporal_linkset_.weighted_size / denom
        else:
            return .0

    @property
    def temporal_nodeset_coverage(self):
        """Calculate the coverage of the temporal-node-set.

        Parameters
        ----------
        None. Property.

        Returns
        -------
        ns_coverage : Real
            Returns :math:`\\frac{|W|}{|V\\times T|}`

        """
        denom = float(self.timeset_.size * self.nodeset_.size)
        if denom > .0:
            return self.temporal_nodeset_.size / denom
        else:
            return .0

    def time_coverage_node(self, u=None):
        """Calculate the time coverage of a node inside the stream_graph.

        Parameters
        ----------
        u: NodeId or None

        Returns
        -------
        time_coverage_node : Real or NodeCollection(Real)
            Returns :math:`\\frac{|T_{u}|}{|T|}`.
            If u is None, returns a dictionary of all nodes and their coverages.

        """
        denom = float(self.timeset_.size)
        if u is None:
            def fun(x, y):
                if denom != .0:
                    return y/denom
                else:
                    return .0
            return self.temporal_nodeset_.node_duration().map(fun)
        else:
            if denom == .0:
                return .0
            else:
                return self.temporal_nodeset_.node_duration(u) / denom

    def time_coverage_link(self, l=None, weights=False, direction='out'):
        """Calculate the time coverage of a link inside the stream_graph.

        Parameters
        ----------
        l: (NodeId, NodeId) or None

        direction: 'in', 'out' or 'both', default='out'

        Returns
        -------
        time_coverage : Real or LinkCollection
            Returns :math:`\\frac{|T_{uv}|}{|T_{u} \\cap T_{v}|}`.
            If l is None, returns a dictionary of all links and their coverages.

        """
        if l is None:
            times = self.temporal_linkset_.duration_of(direction=direction, weights=weights)
            active_links = set(k for k, v in times if v > .0)
            common_times = self.temporal_nodeset_.common_time_pair(l=active_links)
            def fun(k, v):
                return (times[k]/float(v) if v > .0 else .0)
            return common_times.map(fun)
        else:
            denom = float(self.temporal_nodeset_.common_time_pair(l))
            if denom == .0:
                return .0
            else:
                return self.temporal_linkset_.duration_of(l, direction=direction, weights=weights) / denom

    def node_coverage_at(self, t=None):
        """Calculate the node coverage of a time instant inside the stream_graph.

        Parameters
        ----------
        t: time or None

        Returns
        -------
        node_coverage : Real or TimeCollection
            Returns :math:`\\frac{|V_{t}|}{|V|}`.
            If None returns the time coverage for each node at each time-event.

        """
        denom = float(self.nodeset_.size )
        if t is None:
            if denom > .0:
                def fun(t, v):
                    return v/denom
                return self.temporal_nodeset_.n_at().map(fun)
            else:
                return TimeCollection(instants=self.nodeset_.nodes_at().instants)
        else:
            denom = float(self.nodeset_.size )
            if denom > .0:
                return self.temporal_nodeset_.n_at(t) / denom
            else:
                return .0

    def link_coverage_at(self, t=None, weights=False):
        """Calculate the link coverage of a time instant inside the stream_graph.

        Parameters
        ----------
        t: time or None

        Returns
        -------
        link_coverage : Real or TimeCollection
            Returns :math:`\\frac{|E_{t}|}{|V(t)|^{2}}`.
            Returns the time collection for each link at each time-event.

        """
        if t is None:
            ns, ms = self.temporal_nodeset_.n_at(t=None), self.temporal_linkset_.m_at(t=None, weights=weights)
            def fun(x, y):
                return ((2*y / float(x**2)) if x != .0 else .0)
            return ns.merge(ms, fun, missing_value=.0)
        else:
            denom = float(self.temporal_nodeset_.n_at(t) ** 2)
            if denom > .0:
                return 2 * self.temporal_linkset_.m_at(t, weights=weights) / denom
            else:
                return .0

    def neighbor_coverage(self, u=None, direction='out', weights=False):
        """Calculate the neighbor coverage of a node inside the stream_graph.

        Parameters
        ----------
        u: NodeId or None

        direction: 'in', 'out' or 'both', default='out'

        weigths: bool, default=False

        Returns
        -------
        neighbor_coverage : Real or dict
            Returns :math:`\\frac{|N(u)|}{\\sum_{v\\in V}{|T_{u}\\cap T_{v}|}}`
            If u is None, returns a dictionary of all nodes and their neighbor coverages.

        """
        if u is None:
            m = self.temporal_linkset_.degree_of(direction=direction, weights=weights)
            active_nodes = set(k for k, v in m if v > .0)
            common_times = dict(self.temporal_nodeset_.common_time(u=active_nodes))
            # maybe add a u = nodes argument in temporal_nodeset_common_times
            def fun(x, y):
                ct = common_times.get(x, .0)
                if y > .0 and ct > .0:
                    return y/float(ct)
                return y/common_times[x]
            return m.map(fun)
        else:
            denom = float(self.temporal_nodeset_.common_time(u))
            if denom == .0:
                return .0
            else:
                return self.temporal_linkset_.degree_of(u, direction, weights=weights) / denom

    def neighbor_coverage_at(self, u=None, t=None, direction='out', weights=False):
        """Calculate the coverage of a node inside the stream_graph.

        Parameters
        ----------
        u: NodeId or None

        t: Time or None

        direction: 'in', 'out' or 'both', default='out'

        Returns
        -------
        time_coverage : Real
            Returns :math:`\\frac{|N_{t}(u)|}{|V|}`.
            If u is None return the neighbor coverage for each node at time t.
            If t is None return the neighbor coverage for node u for all time-events.
            If u and t are None return the neighbor coverage for each node at each time-event.

        """
        def coverage(x, y):
            return (x/float(y) if y!=.0 else .0)
        if t is None:
            n = self.temporal_nodeset_.n_at(None)
            if u is None:
                neighbors = self.temporal_linkset_.degree_at(None, None, direction, weights)
                def fun(x, y):
                    return y.merge(n, coverage)
                return neighbors.map(fun)
            else:
                return self.temporal_linkset_.degree_at(u, None, direction, weights).merge(n, coverage)
        else:
            denom = float(self.temporal_nodeset_.n_at(t))
            if u is None:
                def fun(x, y):
                    return coverage(y, denom)
                return self.temporal_linkset_.degree_at(None, t, direction, weights).map(fun)
            else:
                if denom > .0:
                    return self.temporal_linkset_.degree_at(u, t, direction, weights) / denom
                else:
                    return .0

    def mean_degree_at(self, t=None, weights=False):
        """Calculate the mean degree at a give time.

        Parameters
        ----------
        t: Time or None

        Returns
        -------
        time_coverage : Real or TimeCollection
            Returns :math:`\\frac{|E_{t}|}{|V_{t}|}`
            Returns mean degree at each time t.

        """
        if t is None:
            if bool(self):
                ns, ms = self.temporal_nodeset_.n_at(t=None), self.temporal_linkset_.m_at(t=None, weights=weights)
                def fun(x=.0, y=.0):
                    return (y / x if x != .0 and y != .0 else .0)
                return ns.merge(ms, fun, missing_value=.0)
            return list()
        else:
            denom = float(self.temporal_nodeset_.n_at(t))
            if denom > .0:
                return self.temporal_linkset_.m_at(t, weights=weights) / denom
            else:
                return .0

    def __and__(self, sg):
        if isinstance(sg, StreamGraph):
            return StreamGraph(self.nodeset_ & sg.nodeset_,
                               self.timeset_ & sg.timeset_,
                               self.temporal_nodeset_ & sg.temporal_nodeset_,
                               self.temporal_linkset_ & sg.temporal_linkset_)
        else:
            raise UnrecognizedStreamGraph('right operand')

    def __or__(self, sg):
        if isinstance(sg, StreamGraph):
            return StreamGraph(self.nodeset_ | sg.nodeset_,
                               self.timeset_ | sg.timeset_,
                               self.temporal_nodeset_ | sg.temporal_nodeset_,
                               self.temporal_linkset_ | sg.temporal_linkset_)
        else:
            raise UnrecognizedStreamGraph('right operand')

    def __sub__(self, sg):
        if isinstance(sg, StreamGraph):
            nsm = self.temporal_nodeset_ - sg.temporal_nodeset_
            return StreamGraph((self.nodeset_ - sg.nodeset_) | nsm.nodeset,
                               self.timeset_ - sg.timeset_, nsm,
                               self.temporal_linkset_ - sg.temporal_linkset_)
        else:
            raise UnrecognizedStreamGraph('right operand')

    def issuperset(self, sg):
        if isinstance(sg, StreamGraph):
            return (self.nodeset_.issuperset(sg.nodeset_) and
                    self.timeset_.issuperset(sg.timeset_) and
                    self.temporal_nodeset_.issuperset(sg.temporal_nodeset_) and
                    self.temporal_linkset_.issuperset(sg.temporal_linkset_))
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
            Returns :math:`\\frac{|W|}{|T|}`

        """
        denom = float(self.timeset_.size)
        if denom > .0:
            return self.temporal_nodeset_.size/denom
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
            Returns :math:`\\frac{|E|}{|T|}`

        """
        denom = float(self.timeset_.size)
        if denom > .0:
            return self.temporal_linkset_.size/denom
        else:
            return .0

    def induced_substream(self, tns):
        """Calculate the induced substream of the stream-graph from a TemporalNodeSet.

        Parameters
        ----------
        tns: TemporalNodeSet

        Returns
        -------
        stream_graph : StreamGrpah
            Returns the induced substream.

        """
        assert isinstance(ns, ABC.TemporalNodeSet)
        tns_is = self.temporal_nodeset_ & tns
        tls_ind = self.temporal_linkset_.induced_substream(tns_is)
        return StreamGraph(self.nodeset, self.timeset, tns_is, tls_ind)

    def discretize(self, bins=None, bin_size=None):
        """Returns a discrete version of the current TemporalLinkSet.

        Parameters
        ----------
        bins : Iterable or None.
            If None, step should be provided.
            If Iterable it should contain n+1 elements that declare the start and the end of all (continuous) bins.

        bin_size : Int or datetime
            If bins is provided this argument is ommited.
            Else declare the size of each bin.

        Returns
        -------
        timeset_discrete : TimeSet
            Returns a discrete version of the TimeSet.

        bins : list
            A list of the created bins.

        """
        if self.discrete:
            timeset, bins = self.timeset_.discretize(bins, bin_size)
            tns, _ = self.temporal_nodeset_.discretize(bins=bins)
            tls, _ = self.temporal_linkset_.discretize(bins=bins)
            return self.__class__(self.nodeset, timeset, tns, tls), bins
        else:
            warn('Stream-Graph is already discrete')
            return self

    @property
    def aggregated_graph(self):
        from stream_graph import Graph
        return Graph(self.nodeset_, self.linkset)


    @property
    def data_cube(self):
        if not bool(self):
            return DataCube()
        if self.discrete:
            column_sizes = {('u',): self.nodeset_.size,
                            ('v',): self.nodeset_.size,
                            ('ts',): self.timeset_.size,
                            ('u', 'ts'): self.temporal_nodeset_.size,
                            ('v', 'ts'): self.temporal_nodeset_.size}
            if not isinstance(self.temporal_linkset_, ABC.ITemporalLinkSet):
                iter_ = iter((u, v, t) for (u, v, ts, tf) in self.temporal_linkset_ for t in range(ts, tf+1))
            else:
                iter_ = iter(self.temporal_linkset_)
            return DataCube(iter_, columns=['u', 'v', 'ts'], column_sizes=column_sizes)
        else:
            raise ValueError('Stream-Graph should be discrete to be convertible to a data-cube')

