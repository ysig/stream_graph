import abc
import copy

# 2/3 Cross Compatibility
ABC = abc.ABCMeta('ABC', (object,), {'__slots__': ()}) 

class NodeStream(ABC):
    """NodeStream Object API Specification.
    
    A NodeStream can be abstractly be defined as a set of nodes :code:`u` associated with a set of time intervals :code:`(ts, tf)`.

    """

    @property
    @abc.abstractmethod
    def nodeset(self):
        """Return the nodeset that can be derived from this NodeStream.
        
        Parameters
        ----------
        None. Property.
        
        Returns
        -------
        nodeset : NodeSet
            Returns all the distinct nodes that appear inside the NodeStream.

        """
        pass

    @property
    @abc.abstractmethod
    def timeset(self):
        """Return the timeset that can be derived from this NodeStream.
        
        Parameters
        ----------
        None. Property.
        
        Returns
        -------
        timeset : TimeSet
            Returns all the times that nodes appear inside the NodeStream.

        """
        pass

    @property
    @abc.abstractmethod
    def size(self):
        """Returns the size of the NodeStream.
        
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
        """Returns the total time that each node shares with all the other nodes of the NodeStream.
        
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
        """Returns number of nodes of the NodeStream.
        
        Parameters
        ----------
        None. Property.
        
        Returns
        -------
        n : Int
            Returns the number of distinct nodes in the NodeStream.

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

    def node_duration(self, u):
        """Returns the duration of a node.
        
        Parameters
        ----------
        u : Node_Id
        

        Returns
        -------
        duration : Real
            The total amount of time that a node exist.
        
        """
        return self.times_of(u).size

    @abc.abstractmethod
    def common_time(self, u, v=None):
        """Returns the common_time between a node and one other or All.
        
        Parameters
        ----------
        u : Node_Id
        
        v : Node_Id or None

        Returns
        -------
        total_time : Real
            If :code:`v` is `None`, returns :math:`\sum_{u,v\in V,\; u\\neq v} |T_{u} \cap T_{v}|`.
            Else, return :math:`|T_{u} \cap T_{v}|`.

        """
        pass

    @abc.abstractmethod
    def nodes_at(self, t):
        """Return the nodes at a certain time.
        
        Parameters
        ----------
        t : Real
        
        Returns
        -------
        nodes : NodeSet
            Active nodes at time t.
        
        """
        pass

    @abc.abstractmethod
    def times_of(self, u):
        """Returns TimeSet that a nodes appears in the NodeStream.
        
        Parameters
        ----------
        u : Node_Id
        
        Returns
        -------
        timeset : TimeSet
            Returns the times that node :code:`u` exists.

        """
        pass

    def n_at(self, t):
        """Returns number of nodes of the NodeStream at a certain time.
        
        Parameters
        ----------
        t : Real
        
        Returns
        -------
        n : Int
            Returns the number of active nodes at time :code:`t`.

        """
        return self.nodes_at(t).size

    @abc.abstractmethod
    def issuperset(self, ns):
        """Check if a nodestream contains another nodestream.
        
        Parameters
        ----------
        ns : NodeStream
        
        Returns
        -------
        issuperset_f : Bool
            True if all nodes of ns appear in same or lesser intervals.

        """
        pass

    @abc.abstractmethod
    def __iter__(self):
        """Implementation of the :code:`iter` function for a NodeStream object.
        
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
        """Implementation of the :code:`bool` casting of a NodeStream object.
        
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
        """Implementation of the :code:`&` operator for a NodeStream object.
        
        Parameters
        ----------
        ns : NodeStream
        
        Returns
        -------
        out : NodeStream
            Returns the **intersection** of Nodes at Intervals.

        """
        pass

    @abc.abstractmethod
    def __sub__(self, ns):
        """Implementation of the :code:`-` operator for a NodeStream object.
        
        Parameters
        ----------
        ns : NodeStream
        
        Returns
        -------
        out : NodeStream
            Returns the **difference** of Nodes at Intervals.

        """

        pass

    @abc.abstractmethod
    def __or__(self, ns):
        """Implementation of the :code:`|` operator for a NodeStream object.
        
        Parameters
        ----------
        ns : NodeStream
        
        Returns
        -------
        out : NodeStream
            Returns the **union** of Nodes at Intervals.

        """
        pass

    def copy(self, deep=True):
        """Returns a deep or shallow copy of the current NodeStream.
        
        Parameters
        ----------
        deep : Bool
        
        Returns
        -------
        nodestream_copy : NodeStream
            Returns a deep or shallow copy of the current NodeStream.

        """
        if deep:
            return copy.deepcopy(self)
        else:
            return copy.copy(self)

class INodeStream(NodeStream):
    @property
    def total_common_time(self):
        return 0

    @property
    def size(self):
        return 0
    
    def node_duration(self, u):
        return 0

    def common_time(self, u, v=None):
        return 0.
