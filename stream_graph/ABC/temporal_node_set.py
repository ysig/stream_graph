from __future__ import absolute_import
import abc
import copy

from itertools import combinations
from warnings import warn
from collections import Iterable
from six import string_types
from .utils import ABC_to_string

# 2/3 Cross Compatibility
ABC = abc.ABCMeta('ABC', (object,), {'__slots__': ()})


class TemporalNodeSet(ABC):
    """TemporalNodeSet Object API Specification.

    A TemporalNodeSet can be abstractly be defined as a set of nodes :code:`u` associated with a set of time intervals :code:`(ts, tf)`.

    """

    def __str__(self):
        return ABC_to_string(self, ['u'])

    @property
    def instantaneous(self):
        """Defines if the Time Set is instantaneous."""
        return False

    @property
    @abc.abstractmethod
    def discrete(self):
        """Designate if the TemporalNodeSet is on discrete Time.

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
    def nodeset(self):
        """Return the nodeset that can be derived from this TemporalNodeSet.

        Parameters
        ----------
        None. Property.

        Returns
        -------
        nodeset : NodeSet
            Returns all the distinct nodes that appear inside the TemporalNodeSet.

        """
        pass

    @property
    @abc.abstractmethod
    def timeset(self):
        """Return the timeset that can be derived from this TemporalNodeSet.

        Parameters
        ----------
        None. Property.

        Returns
        -------
        timeset : TimeSet
            Returns all the times that nodes appear inside the TemporalNodeSet.

        """
        pass

    @property
    @abc.abstractmethod
    def size(self):
        """Returns the size of the TemporalNodeSet.

        Parameters
        ----------
        None. Property.


        Returns
        -------
        size : Real
            The size :math:`\sum_{u} |T_{u}|` of the stream_graph.

        """
        pass

    @property
    @abc.abstractmethod
    def total_common_time(self):
        """Returns the total time that each node shares with all the other nodes of the TemporalNodeSet.

        Parameters
        ----------
        None. Property.


        Returns
        -------
        size : Real
            The size :math:`\sum_{uv\in V\otimes V} |T_{u} \cap T_{v}|` of the stream_graph.

        """
        pass

    @property
    def n(self):
        """Returns number of nodes of the TemporalNodeSet.

        Parameters
        ----------
        None. Property.

        Returns
        -------
        n : Int
            Returns the number of distinct nodes in the TemporalNodeSet.

        """
        return self.nodeset.size

    @property
    def total_time(self):
        """Returns the size of the derived TimeSet.

        Parameters
        ----------
        None. Property.


        Returns
        -------
        total_time : Real
            The total amount of time from the union of intervals that nodes exist.

        """
        return self.timeset.size

    @abc.abstractmethod
    def __contains__(self, u):
        """Implementation of the :code:`in` operator for LinkStream.

        Parameters
        ----------
        l : tuple, len(l) == 2
            l[0] : :code:`u` or None
            l[1] : :code:`t` or :code:`(ts, tf)` or None

        Returns
        -------
        contains : Bool
            Returns True if a node :code:`u` exists at :code:`t` or through **all** :code:`(ts, tf)`.
            If an element is :code:`None`, :code:`in` will return True if the other elements matches.
            If both elements are :code:`None` output will be None.

        """
        pass

    def duration_of(self, u=None):
        """Returns the duration of a node.

        Parameters
        ----------
        u : Node_Id or None

        Returns
        -------
        duration : Real or NodeCollection(Real)
            The total amount of time that a node exist.
            If None, returns a NodeCollection with the durations for all nodes in the temporal_nodeset.

        """
        if u is None:
            from stream_graph.collections import NodeCollection
            return NodeCollection({u: self.times_of(u).size for u in self.nodeset})
        else:
            return self.times_of(u).size

    @abc.abstractmethod
    def common_time(self, u=None):
        """Returns the common_time between a node and one other or all.

        Parameters
        ----------
        u : Node_Id or set(Node_Id) or None

        Returns
        -------
        total_time : Real or NodeCollection(Real)
            If :code:`u` is a Node_Id, returns :math:`\sum_{v\in V,\; v\\neq u} |T_{u} \cap T_{v}|`.
            If None it returns a NodeCollection of :math:`\sum_{v\in V,\; v\\neq u} |T_{u} \cap T_{v}|` for each u.
            If u is a set of Node_Id the above is restricted onto the provided Nodes.

        """
        pass


    @abc.abstractmethod
    def common_time_pair(self, l=None):
        """Returns the common_time between a pair of nodes.

        Parameters
        ----------
        l : (Node_Id, Node_Id) or set((Node_Id, Node_Id)) or None

        Returns
        -------
        total_time : Real or LinkCollection(Real)
            If l is (Node_Id, Node_Id), return :math:`|T_{u} \cap T_{v}|`.
            If None, return a LinkCollection for all (u, v) with :math:`|T_{u} \cap T_{v}|`.
            If u is a set of Node_Id the above is restricted onto the provided Node airs.

        """
        pass

    @abc.abstractmethod
    def nodes_at(self, t=None):
        """Return the nodes at a certain time.

        Parameters
        ----------
        t : Real or None

        Returns
        -------
        nodes : NodeSet or TimeGenerator(NodeSet)
            Active nodes at time t.
            If None returns an TimeGenerator of tuples containing a timestamp and a NodeSet.

        """
        pass

    @abc.abstractmethod
    def times_of(self, u=None):
        """Returns TimeSet that a nodes appears in the TemporalNodeSet.

        Parameters
        ----------
        u : Node_Id or None

        Returns
        -------
        timeset : TimeSet or NodeCollection(TimeSet)
            Returns the times that node :code:`u` exists.
            Return a NodeCollection of the TimeSet for each node u.

        """
        pass

    def n_at(self, t=None):
        """Returns number of nodes of the TemporalNodeSet at a certain time.

        Parameters
        ----------
        t : Real

        Returns
        -------
        n : Int or TimeCollection(Int)
            Returns the number of active nodes at time :code:`t`.
            If None returns an iterator of tuples containing a timestamp and an Int.

        """
        if t is None: # Wrong
            from stream_graph.collections import TimeCollection
            time_nodes = self.nodes_at()
            return TimeCollection([(ts, ns.size) for (ts, ns) in time_nodes], time_nodes.instants)
        else:
            return self.nodes_at(t).size

    def _common_time_pair__list_input(self, l):
        return (isinstance(l, Iterable) and
                not (isinstance(l, tuple) and
                     len(l) == 2 and
                     any(not isinstance(a, Iterable) or isinstance(a, string_types) for a in l)))

    def _common_time__list_input(self, u):
        return isinstance(u, Iterable) and not isinstance(u, string_types)

    @abc.abstractmethod
    def issuperset(self, ns):
        """Check if a temporal_nodeset contains another temporal_nodeset.

        Parameters
        ----------
        ns : TemporalNodeSet

        Returns
        -------
        issuperset_f : Bool
            True if all nodes of ns appear in same or lesser intervals.

        """
        pass

    @abc.abstractmethod
    def __iter__(self):
        """Implementation of the :code:`iter` function for a TemporalNodeSet object.

        Parameters
        ----------
        None

        Returns
        -------
        out : Iterator of tuple
            Each tuple is of the form :code:`(u, ts, tf)`.

        """
        pass

    @abc.abstractmethod
    def __bool__(self):
        """Implementation of the :code:`bool` casting of a TemporalNodeSet object.

        Parameters
        ----------
        None.

        Returns
        -------
        out : Bool
            Return True if an object is **both** initialized and contains information.

        """
        pass

    # Python2 cross-compatibility
    def __nonzero__(self):
        return self.__bool__()

    @abc.abstractmethod
    def __and__(self, ns):
        """Implementation of the :code:`&` operator for a TemporalNodeSet object.

        Parameters
        ----------
        ns : TemporalNodeSet

        Returns
        -------
        out : TemporalNodeSet
            Returns the **intersection** of Nodes at Intervals.

        """
        pass

    @abc.abstractmethod
    def __sub__(self, ns):
        """Implementation of the :code:`-` operator for a TemporalNodeSet object.

        Parameters
        ----------
        ns : TemporalNodeSet

        Returns
        -------
        out : TemporalNodeSet
            Returns the **difference** of Nodes at Intervals.

        """

        pass

    @abc.abstractmethod
    def __or__(self, ns):
        """Implementation of the :code:`|` operator for a TemporalNodeSet object.

        Parameters
        ----------
        ns : TemporalNodeSet

        Returns
        -------
        out : TemporalNodeSet
            Returns the **union** of Nodes at Intervals.

        """
        pass

    def copy(self, deep=True):
        """Returns a deep or shallow copy of the current TemporalNodeSet.

        Parameters
        ----------
        deep : Bool

        Returns
        -------
        temporal_nodeset_copy : TemporalNodeSet
            Returns a deep or shallow copy of the current TemporalNodeSet.

        """
        if deep:
            return copy.deepcopy(self)
        else:
            return copy.copy(self)

    def discretize(self, bins=None, bin_size=None):
        """Returns a discrete version of the current TemporalNodeSet.

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


class ITemporalNodeSet(TemporalNodeSet):
    """ITemporalNodeSet Object API Specification.

    A Instantaneous Temporal NodeSet can be abstractly be defined as a set of nodes :code:`u` associated with a time-stamp :code:`ts`.

    """

    @property
    def instantaneous(self):
        """Defines if the Time Set is instantaneous."""
        return True

    @property
    def total_common_time(self):
        if self.discrete:
            return self._total_common_time_discrete
        else:
            return 0

    @property
    @abc.abstractmethod
    def _total_common_time_discrete(self):
        pass

    @property
    def size(self):
        if self.discrete:
            return self.number_of_instants
        else:
            return 0

    def duration_of(self, u=None):
        if self.discrete:
            return self._duration_of_discrete(u)
        else:
            if u is None:
                from stream_graph.collections import NodeCollection
                return NodeCollection({u: 0 for u in self.nodeset})
            return 0

    @abc.abstractmethod
    def _duration_of_discrete(self, u=None):
        pass

    def common_time(self, u=None):
        if self.discrete:
            return self._common_time_discrete(u)
        else:
            if u is None:
                from stream_graph.collections import NodeCollection
                return NodeCollection({u: 0 for u in self.nodeset})
            elif self._common_time__list_input(u):
                from stream_graph.collections import NodeCollection
                return NodeCollection({v: 0 for v in u})
            return 0

    @abc.abstractmethod
    def _common_time_discrete(self, u=None):
        pass

    def common_time_pair(self, l=None):
        if self.discrete:
            return self._common_time_pair_discrete(l)
        else:
            if l is None:
                from stream_graph.collections import NodeCollection
                return NodeCollection({l: 0 for l in combinations(self.nodeset, 2)})
            if self._common_time_pair__list_input(l):
                from stream_graph.collections import NodeCollection
                return NodeCollection({l: 0 for u in l})
            return 0

    @abc.abstractmethod
    def _common_time_pair_discrete(self, l=None):
        pass

    @property
    @abc.abstractmethod
    def number_of_instants(self):
        pass
