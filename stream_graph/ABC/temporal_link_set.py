from __future__ import absolute_import
import copy
import abc

from warnings import warn
from stream_graph.collections import TimeCollection

from .utils import ABC_to_string

# 2/3 Cross Compatibility
ABC = abc.ABCMeta('ABC', (object,), {'__slots__': ()})


class TemporalLinkSet(ABC):
    """TemporalLinkSet Object API Specification.

    A TemporalLinkSet can be abstractly be defined as a set of links :code:`(u, v)` associated with a set of intervals :code:`(ts, tf)`.

    """
    def __str__(self):
        return ABC_to_string(self, ['u', 'v'])

    @property
    def instantaneous(self):
        """Defines if the Time Set is instantaneous."""
        return False

    @property
    def weighted(self):
        """Designate if the TemporalLinkSet is weighted.

        Parameters
        ----------
        None. Property.


        Returns
        -------
        discrete : Bool or None
            True if weighted. Returns None if empty.

        """
        pass

    @property
    @abc.abstractmethod
    def discrete(self):
        """Designate if the TemporalLinkSet is on discrete Time.

        Parameters
        ----------
        None. Property.


        Returns
        -------
        discrete : Bool or None
            True if the time is discrete.
            Returns None if empty.

        """
        pass

    @property
    @abc.abstractmethod
    def size(self):
        """Returns the size of the TemporalLinkSet.

        Parameters
        ----------
        None. Property.


        Returns
        -------
        size : Real
            The size :math:`\sum_{uv} |T_{uv}|` of the stream_graph.

        """
        pass

    @property
    def weighted_size(self):
        """Returns the size of the TemporalLinkSet.

        Parameters
        ----------
        None. Property.


        Returns
        -------
        size : Real
            The size :math:`\sum_{uv} w_{uv}|T_{uv}|` of the stream_graph.

        """
        if not bool(self):
            return 0
        elif self.weighted:
            return self._weighted_size
        else:
            return self.size

    @property
    def _weighted_size(self):
        raise NotImplementedError

    @abc.abstractmethod
    def __contains__(self, l):
        """Implementation of the :code:`in` operator for TemporalLinkSet.

        Parameters
        ----------
        l : tuple, len(l) == 3
            l[0] : :code:`u`
            l[1] : :code:`v`
            l[2] : :code:`t` or :code:`(ts, tf)`

        Returns
        -------
        contains : Bool
            Returns true if the link :code:`(u, v)` exists at :code:`t` or through **all** :code:`(ts, tf)`.

        """
        pass

    @abc.abstractmethod
    def links_at(t=None):
        """Return the links at a certain time.

        Parameters
        ----------
        t : Real or None

        Returns
        -------
        links : LinkSet or TimeGenerator(LinkSet)
            Active links at time t.
            If t is None, return a continuous TimeGenerator of LinkSet

        """
        pass

    @property
    @abc.abstractmethod
    def basic_temporal_nodeset(self):
        """Return the basic temporal_nodeset that contains the temporal-linkstream.

        Parameters
        ----------
        None. Property.

        Returns
        -------
        temporal_nodeset : TemporalNodeSet
            Returns a TemporalNodeSet covering all nodes of the TemporalLinkSet from minimum time to maximum time.

        """
        pass

    @property
    @abc.abstractmethod
    def minimal_temporal_nodeset(self):
        """Return the minimal temporal_nodeset that can be derived from the temporal-linkstream.

        Parameters
        ----------
        None. Property.

        Returns
        -------
        temporal_nodeset : TemporalNodeSet
            Returns a TemporalNodeSet where each node appears only when it exists in at least one link.

        """
        pass

    @property
    @abc.abstractmethod
    def linkset(self):
        """Return the linkset that can be derived from the TemporalLinkSet.

        Parameters
        ----------
        None. Property.

        Returns
        -------
        linkset : LinkSet
            Returns al the links that appear in the TemporalLinkSet.

        """
        pass

    @property
    @abc.abstractmethod
    def timeset(self):
        """Return the timeset that can be derived from the TemporalLinkSet.

        Parameters
        ----------
        None. Property.

        Returns
        -------
        timeset : TimeSet
            Returns all the times where there exists at least one link.

        """
        pass

    @property
    @abc.abstractmethod
    def nodeset(self):
        """Return the nodeset that can be derived from the TemporalLinkSet.

        Parameters
        ----------
        None. Property.

        Returns
        -------
        nodeset : NodeSet
            Returns all the nodes that appear in links.

        """
        pass

    @abc.abstractmethod
    def times_of(self, l=None, direction='out'):
        """Return the times that a link appears.

        Parameters
        ----------
        l : (Node_Id, Node_Id) or None

        direction : string={'in', 'out', 'both'}, default='both'

        Returns
        -------
        timeset : TimeSet or LinkCollection(TimeSet)
            Return the times where a link starting from u to v (direction='out') or from v to u (direction='in') or 'both' exist.
            If l is None, returns the timesets corresponding to all links

        """
        pass

    @abc.abstractmethod
    def neighbors_at(self, u=None, t=None, direction='out'):
        """Return the NodeSet of neighbors of node at a certain time.

        Parameters
        ----------
        u : Node_Id or None

        t : Real, or tuple with len(t)==2 or None
            TimeStamp or Interval.

        direction : string={'in', 'out', 'both'}, default='both'

        Returns
        -------
        nodeset : NodeSet or NodeCollection or TimeGenerator or NodeCollection(TimeSparseCollection)
            Return the ('in', 'out' or 'both') neighbors of node at a certain time(s).
            If u is None return the NodeSet of neighbors for each node at time t.
            If t is None return the NodeSet of neighbors for node u for all times.
            If u and t are None return for each node the NodeSet of neighbors at its time instant.

        """
        pass

    def degree_at(self, u=None, t=None, direction='out', weights=False):
        """Return the degree of a node at a certain time.

        Parameters
        ----------
        u : Node_Id or None

        t : Real, or tuple with len(t)==2 or None
            TimeStamp or Interval.

        direction : string={'in', 'out', 'both'}, default='both'

        weights : bool, default=False

        Returns
        -------
        nodeset : NodeSet or NodeCollection or TimeGenerator or NodeCollection(TimeCollection)
            Return the ('in', 'out' or 'both') degree of node at a certain time(s).
            If u is None return the degree for each node at time t.
            If t is None return the degree for node u for all times.
            If u and t are None return the degree for each node at its time instant.

        """
        if weights and self.weighted:
            return self._degree_at_weighted(u, t, direction)
        else:
            return self._degree_at_unweighted(u, t, direction)

    def degree_of(self, u=None, direction='out', weights=False):
        """Return the time-degree of a node at a certain time.

        Parameters
        ----------
        u : Node_Id

        direction : string={'in', 'out', 'both'}, default='both'

        weights : bool, default=False

        Returns
        -------
        nodeset : NodeCollection or Real
            Return the ('in', 'out' or 'both') time-degree of a node.
            If u is None return the time-degree for all nodes.

        """
        if weights and self.weighted:
            return self._degree_of_weighted(u, direction)
        else:
            return self._degree_of_unweighted(u, direction)

    def _degree_of_weighted(self, u, direction):
        raise NotImplementedError

    def _degree_of_unweighted(self, u, direction):
        if u is None:
            from stream_graph.collections import NodeCollection
            return NodeCollection({n: tns.size for n, tns in self.neighbors_of(None, direction=direction)})
        else:
            return self.neighbors_of(u, direction=direction).size

    def _degree_at_weighted(self, u, t, direction):
        raise NotImplementedError

    def _degree_at_unweighted(self, u, t, direction):
        if u is None:
            n = self.neighbors_at(u=u, t=t, direction=direction)

            def size(x, y):
                return y.size
            if t is None:
                def map(x, y):
                    return y.map(size)
                return n.map(map)
            else:
                return n.map(size)
        else:
            if t is None:
                return self.neighbors_of(u, direction=direction).n_at()
            else:
                return self.neighbors_at(u=u, t=t, direction=direction).size

    @abc.abstractmethod
    def neighbors_of(self, u=None, direction='out'):
        """Return the temporal_nodeset of a neighbors of a node.

        Parameters
        ----------
        u : Node_Id or None

        direction : string={'in', 'out', 'both'}, default='both'

        Returns
        -------
        temporal_nodeset : TemporalNodeSet or NodeCollection(TemporalNodeSet)
            Return the ('in', 'out' or 'both') temporal_nodeset of neighbors at a certain time(s).
            Return a dictionary of nodes as keys assigned by their neighboring temporal_nodeset.

        """
        pass

    @abc.abstractmethod
    def substream(self, nsu=None, nsv=None, ts=None):
        """Return the subtream occuring from two nodesets and a timeset.

        Parameters
        ----------
        nsu : NodeSet or None

        nsv : NodeSet or None

        ts : TimeSet or None

        Returns
        -------
        temporal_linkstream : TemporalLinkSet
            Return substream containing starting nodes from nsu, end nodes of nsv, contained from the intersection with ts.
            If a nodeset is None, all nodes and considered. If a timeset is None, all time is considered.

        """

    def filter(self, fun):
        """Filter the link stream.

        Parameters
        ----------
        fun : function
            A function that for each a tuple as it is defined from iter, return True or False.

        Returns
        -------
        temporal_linkstream : TemporalLinkSet
            Return sub-linkstream that satisfies the function.

        """
        assert callable(fun)
        return self.__class__([key for key in iter(self) if fun(key)], weighted=self.weighted, discrete=self.discrete)

    @abc.abstractmethod
    def __bool__(self):
        """Implementation of the :code:`bool` casting of a TemporalLinkSet object.

        Parameters
        ----------
        None.

        Returns
        -------
        out : Bool
            Return True if an object is **both** initialized and contains information.

        """
        pass

    @abc.abstractmethod
    def __and__(self, ls):
        """Implementation of the :code:`&` operator of a TemporalLinkSet object.

        Parameters
        ----------
        ls : TemporalLinkSet

        Returns
        -------
        out : TemporalLinkSet
            Returns the **intersection** of Links at Intervals.

        """
        pass

    @abc.abstractmethod
    def __or__(self, ls):
        """Implementation of the :code:`|` operator for a TemporalLinkSet object.

        Parameters
        ----------
        ls : TemporalLinkSet

        Returns
        -------
        out : TemporalLinkSet
            Returns the **union** of Links at Intervals.

        """
        pass

    @abc.abstractmethod
    def __sub__(self, ls):
        """Implementation of the :code:`-` operator for a TemporalLinkSet object.

        Parameters
        ----------
        ls : TemporalLinkSet

        Returns
        -------
        out : TemporalLinkSet
            Returns the **difference** of Links at Intervals.

        """
        pass

    @abc.abstractmethod
    def __iter__(self):
        """Implementation of the :code:`iter` function for a TemporalLinkSet object.

        Parameters
        ----------
        None

        Returns
        -------
        out : Iterator of tuple
            Each tuple is of the form (u, v, ts, tf)

        """
        pass

    @abc.abstractmethod
    def issuperset(self, ls):
        """Check if a TemporalLinkSet contains another TemporalLinkSet.

        Parameters
        ----------
        ls : TemporalLinkSet

        Returns
        -------
        issuperset_f : Bool
            True if all links of ls appear in same or lesser intervals.

        """
        pass

    @abc.abstractmethod
    def induced_substream(self, ns):
        """Returns the induced substream from a TemporalNodeSet.

        Parameters
        ----------
        ns : TemporalNodeSet

        Returns
        -------
        substream : TemporalLinkSet
            Returns the TemporalLinkSet with nodes that appear inside the temporal_nodeset and only for that time.

        """
        pass

    @abc.abstractmethod
    def temporal_neighborhood(self, ns, direction='out'):
        """Returns the neighborhood of a TemporalNodeSet.

        Parameters
        ----------
        ns : TemporalNodeSet

        direction : string={'in', 'out', 'both'}, default='both'

        Returns
        -------
        neighborhood : TemporalNodeSet
            Returns the temporal_nodeset of neighbors of nodes inside ns for a set amount of time.

        """
        pass

    @property
    def m(self):
        """Returns number of links of the TemporalLinkSet.

        Parameters
        ----------
        None. Property.

        Returns
        -------
        m : Int
            Returns the number of links in the TemporalLinkSet.

        """
        return self.linkset.size

    def copy(self, deep=True):
        """Returns a deep or shallow copy of the current TemporalLinkSet.

        Parameters
        ----------
        deep : Bool

        Returns
        -------
        temporal-linkstream_copy : TemporalLinkSet
            Returns a deep or shallow copy of the current TemporalLinkSet.

        """
        if deep:
            return copy.deepcopy(self)
        else:
            return copy.copy(self)

    def m_at(self, t=None, weights=False):
        """Returns the number of links appearing at a certain time.

        Parameters
        ----------
        t : Int

        weights : bool, default=False

        Returns
        -------
        m : Int
            Returns the numer of links at a certain time.

        """
        if not bool(self):
            if t is None:
                return TimeCollection(discrete=self.discrete_)
            else:
                return .0
        elif weights and self.weighted:
            return self._m_at_weighted(t)
        else:
            return self._m_at_unweighted(t)

    def _m_at_weighted(self, t):
        if t is None:
            from stream_graph.collections import TimeCollection
            time_links = self.links_at()
            return TimeCollection([(time, l.weighted_size) for time, l in time_links], time_links.instants)
        else:
            return self.links_at(t).weighted_size

    def _m_at_unweighted(self, t):
        if t is None:
            from stream_graph.collections import TimeCollection
            time_links = self.links_at()
            return TimeCollection([(time, l.size) for time, l in time_links], time_links.instants)
        else:
            return self.links_at(t).size

    def duration_of(self, l=None, direction='out', weights=False):
        """Returns the total duration of a link.

        Parameters
        ----------
        l : (Node_Id, Node_Id) or None

        direction : string={'in', 'out', 'both'}, default='both'

        weights : bool, default=False

        Returns
        -------
        time : Real or dict
            Returns the the total time that link(s) (u, v)[direction='out'], (v, u)[direction='in'] or 'both' (u, v) and (v, u) appear.
            If l is None returns a dictionary of all links with their times.

        """
        if weights and self.weighted:
            self._duration_of_weighted(l, direction)
        else:
            self._duration_of_unweighted(l, direction)

    def _duration_of_weighted(self, l, direction):
        raise NotImplementedError

    def _duration_of_unweighted(self, l, direction):
        if l is None:
            from stream_graph.collections import LinkCollection
            times = self.times_of(None, direction=direction)
            return LinkCollection({l: t.size for l, t in times})
        else:
            return self.times_of(l=l, direction=direction).size

    @abc.abstractmethod
    def get_maximal_cliques(self, direction='both'):
        """Returns the maximal cliques of the temporal-linkstream.

        Parameters
        ----------
        direction : string={'in', 'out', 'both'}, default='both'

        Returns
        -------
        maximal_cliques : set
            Returns a set of tuples containing a frozenset of clique nodes and a tuple of the interval this nodes are active.
        """
        pass

    @property
    def as_stream_graph_basic(self):
        """Generate the basic stream graph containing this TemporalLinkSet.

        Parameters
        ----------
        None. Property.

        Returns
        -------
        stream_graph : StreamGraph
            Generates a StreamGraph containing the basic temporal_nodeset and time range that contain this TemporalLinkSet.

        """
        from stream_graph import StreamGraph
        nsm = self.basic_temporal_nodeset
        return StreamGraph(self.nodeset, nsm.timeset, nsm, self)

    @property
    def as_stream_graph_minimal(self):
        """Generate the minimal stream graph containing this TemporalNodeSet.

        Parameters
        ----------
        None. Property.

        Returns
        -------
        stream_graph : StreamGraph
            Generate the StreamGraph containing the minimal TemporalNodeSet and the minimal TimeSet.

        """
        from stream_graph.base.stream_graph import StreamGraph
        return StreamGraph(self.nodeset, self.timeset, self.minimal_temporal_nodeset, self)

    # Python2 cross-compatibility
    def __nonzero__(self):
        return self.__bool__()

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
        if not self.discrete:
            return self._to_discrete(bins, bin_size)
        else:
            warn(str(self.__class__) + ' is already discrete')
            return self, bins

    @abc.abstractmethod
    def _to_discrete(self, bins, bin_size):
        pass


class ITemporalLinkSet(TemporalLinkSet):
    """Instantaneous Temporal LinkSet Object API Specification.

    A ITemporalLinkSet can be abstractly be defined as a set of links :code:`(u, v)` associated with a timestamp :code:`ts`.

    """

    @property
    def instantaneous(self):
        """Defines if the Time Set is instantaneous."""
        return True

    @property
    def size(self):
        """Returns the size of the TemporalLinkSet.

        Parameters
        ----------
        None. Property.


        Returns
        -------
        size : Int
            If discrete returns total number of interactions.
            Else returns zero.

        """
        if self.discrete:
            return self.number_of_interactions
        else:
            return .0

    @property
    @abc.abstractmethod
    def number_of_interactions(self):
        pass

    @property
    @abc.abstractmethod
    def _weighted_number_of_interactions(self):
        pass

    @property
    def weighted_number_of_interactions(self):
        if self.weighted:
            return self._weighted_number_of_interactions
        else:
            return self.number_of_interactions

    @property
    def _weighted_size(self):
        """Returns the size of the TemporalLinkSet.

        Parameters
        ----------
        None. Property.


        Returns
        -------
        size : Int
            If discrete returns the sum of weights of all interactions.
            Else returns zero.

        """
        if self.discrete:
            return self.weighted_number_of_interactions
        else:
            return .0

    def duration_of(self, l=None, direction='out', weights=False):
        """Returns the total duration of a link.

        Parameters
        ----------
        l : (Node_Id, Node_Id) or None

        direction : string={'in', 'out', 'both'}, default='both'

        Returns
        -------
        time : Real or dict
            Returns the the total time that link(s) (u, v)[direction='out'], (v, u)[direction='in'] or 'both' (u, v) and (v, u) appear.
            If l is None returns a dictionary of all links with their times.

        """
        if self.discrete:
            return self._duration_of_discrete(l=l, direction=direction)
        else:
            if l is None:
                from stream_graph.collections import LinkCollection
                if self.weighted:
                    return LinkCollection({l[:2]: 0. for l in self.linkset})
                else:
                    return LinkCollection({l: 0. for l in self.linkset})
            else:
                return .0

    def degree_of(self, u=None, direction='out', weights=False):
        """Return the time-degree of a node at a certain time.

        Parameters
        ----------
        u : Node_Id

        direction : string={'in', 'out', 'both'}, default='both'

        weights : bool, default=False

        Returns
        -------
        degree : Number or LinkCollection
            Return the ('in', 'out' or 'both') time-degree of a node.
            If u is None return the degree for all node at time t.

        """
        if self.discrete:
            if self.weighted and weights:
                return self._degree_of_discrete_weights(u=u, direction=direction)
            else:
                return self._degree_of_discrete(u=u, direction=direction)
        else:
            if u is None:
                from stream_graph.collections import NodeCollection
                return NodeCollection({u: 0. for u in self.nodeset})
            else:
                return .0

    @abc.abstractmethod
    def _degree_of_discrete(self, u, direction):
        pass

    @abc.abstractmethod
    def _duration_of_discrete(self, l=None, direction='out'):
        pass
