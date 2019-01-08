import copy
import abc

# 2/3 Cross Compatibility
ABC = abc.ABCMeta('ABC', (object,), {'__slots__': ()}) 

class LinkStream(ABC):
    """LinkStream Object API Specification.
    
    A LinkStream can be abstractly be defined as a set of links :code:`(u, v)` associated with a set of intervals :code:`(ts, tf)`.

    """

    @property
    @abc.abstractmethod
    def size(self):
        """Returns the size of the LinkStream.
        
        Parameters
        ----------
        None. Property.
        

        Returns
        -------
        size : Real
            The size :math:`\sum_{uv} |T_{uv}|` of the stream_graph.
        
        """
        pass

    @abc.abstractmethod
    def __contains__(self, l):
        """Implementation of the :code:`in` operator for LinkStream.
        
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
    def links_at(t):
        """Return the links at a certain time.
        
        Parameters
        ----------
        t : Real
        
        Returns
        -------
        links : LinkSet
            Active links at time t.
        
        """
        pass

    @property
    @abc.abstractmethod
    def basic_nodestream(self):
        """Return the basic nodestream that contains the linkstream.
        
        Parameters
        ----------
        None. Property.
        
        Returns
        -------
        nodestream : NodeStream
            Returns a NodeStream covering all nodes of the LinkStream from minimum time to maximum time.
        
        """
        pass

    @property
    @abc.abstractmethod    
    def minimal_nodestream(self):
        """Return the minimal nodestream that can be derived from the linkstream.
        
        Parameters
        ----------
        None. Property.
        
        Returns
        -------
        nodestream : NodeStream
            Returns a NodeStream where each node appears only when it exists in at least one link.

        """
        pass

    @property
    @abc.abstractmethod
    def linkset(self):
        """Return the linkset that can be derived from the LinkStream.
        
        Parameters
        ----------
        None. Property.
        
        Returns
        -------
        linkset : LinkSet
            Returns al the links that appear in the LinkStream.

        """
        pass

    @property
    @abc.abstractmethod
    def timeset(self):
        """Return the timeset that can be derived from the LinkStream.
        
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
        """Return the nodeset that can be derived from the LinkStream.
        
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
    def times_of(self, u, v, direction='out'):
        """Return the times that a link appears.
        
        Parameters
        ----------
        u : Node_Id

        v : Node_Id

        direction : string={'in', 'out', 'both'}, default='both'
        
        Returns
        -------
        timeset : TimeSet
            Return the times where a link starting from u to v (direction='out') or from v to u (direction='in') or 'both' exist.

        """
        pass
    
    @abc.abstractmethod
    def neighbors_at(self, u, t, direction='out'):
        """Return the NodeSet of neighbors of node at a certain time.
        
        Parameters
        ----------
        u : Node_Id

        t : Real, or tuple with len(t)==2
            TimeStamp or Interval.

        direction : string={'in', 'out', 'both'}, default='both'
        
        Returns
        -------
        nodeset : NodeSet
            Return the ('in', 'out' or 'both') neighbors of node at a certain time(s).

        """
        pass

    @abc.abstractmethod
    def neighbors(self, u, direction='out'):
        """Return the nodestream of a neighbors of a node.
        
        Parameters
        ----------
        u : Node_Id

        direction : string={'in', 'out', 'both'}, default='both'
        
        Returns
        -------
        nodestream : NodeStream
            Return the ('in', 'out' or 'both') nodestream of neighbors at a certain time(s).

        """
        pass

    @abc.abstractmethod
    def substream(self, nsu, nsv, ts):
        """Return the subtream occuring from two nodesets and a timeset.
        
        Parameters
        ----------
        nsu : NodeSet

        nsv : NodeSet

        ts : TimeSet
        
        Returns
        -------
        linkstream : LinkStream
            Return substream containing starting nodes from nsu, end nodes of nsv, contained from the intersection with ts.

        """
        pass

    @abc.abstractmethod
    def __bool__(self):
        """Implementation of the :code:`bool` casting of a LinkStream object.
        
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
        """Implementation of the :code:`&` operator of a LinkStream object.
        
        Parameters
        ----------
        ls : LinkStream
        
        Returns
        -------
        out : LinkStream
            Returns the **intersection** of Links at Intervals.

        """
        pass

    @abc.abstractmethod
    def __or__(self, ls):
        """Implementation of the :code:`|` operator for a LinkStream object.
        
        Parameters
        ----------
        ls : LinkStream
        
        Returns
        -------
        out : LinkStream
            Returns the **union** of Links at Intervals.

        """
        pass

    @abc.abstractmethod
    def __sub__(self, ls):
        """Implementation of the :code:`-` operator for a LinkStream object.
        
        Parameters
        ----------
        ls : LinkStream
        
        Returns
        -------
        out : LinkStream
            Returns the **difference** of Links at Intervals.

        """
        pass

    @abc.abstractmethod
    def __iter__(self):
        """Implementation of the :code:`iter` function for a LinkStream object.
        
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
        """Check if a LinkStream contains another LinkStream.
        
        Parameters
        ----------
        ls : LinkStream
        
        Returns
        -------
        issuperset_f : Bool
            True if all links of ls appear in same or lesser intervals.

        """
        pass

    @abc.abstractmethod
    def induced_substream(self, ns):
        """Returns the induced substream from a NodeStream.
        
        Parameters
        ----------
        ns : NodeStream
        
        Returns
        -------
        substream : LinkStream
            Returns the LinkStream with nodes that appear inside the nodestream and only for that time.

        """
        pass

    @abc.abstractmethod
    def neighborhood(self, ns, direction='out'):
        """Returns the neighborhood of a NodeStream.
        
        Parameters
        ----------
        ns : NodeStream
        
        direction : string={'in', 'out', 'both'}, default='both'
        
        Returns
        -------
        neighborhood : NodeStream
            Returns the nodestream of neighbors of nodes inside ns for a set amount of time.  

        """
        pass

    @property
    def m(self):
        """Returns number of links of the LinkStream.
        
        Parameters
        ----------
        None. Property.
        
        Returns
        -------
        m : Int
            Returns the number of links in the LinkStream.

        """
        return self.linkset.size

    def copy(self, deep=True):
        """Returns a deep or shallow copy of the current LinkStream. 
        
        Parameters
        ----------
        deep : Bool
        
        Returns
        -------
        linkstream_copy : LinkStream
            Returns a deep or shallow copy of the current LinkStream.

        """
        if deep:
            return copy.deepcopy(self)
        else:
            return copy.copy(self)

    def m_at(self, t):
        """Returns the number of links appearing at a certain time.
        
        Parameters
        ----------
        t : Int
        
        Returns
        -------
        m : Int
            Returns the numer of links at a certain time.

        """
        return self.links_at(t).size

    def link_duration(self, u, v, direction='out'):
        """Returns the total duration of a link.
        
        Parameters
        ----------
        u : Node_Id

        v : Node_Id

        direction : string={'in', 'out', 'both'}, default='both'
        
        Returns
        -------
        time : Real
            Returns the the total time that link(s) (u, v)[direction='out'], (v, u)[direction='in'] or 'both' (u, v) and (v, u) appear.

        """
        return self.times_of(u, v, direction=direction).size

    @abc.abstractmethod
    def get_maximal_cliques(self, direction='both'):
        """Returns the maximal cliques of the linkstream.
        
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
        """Generate the basic stream graph containing this LinkStream.
        
        Parameters
        ----------
        None. Property.
        
        Returns
        -------
        stream_graph : StreamGraph
            Generates a StreamGraph containing the basic nodestream and time range that contain this LinkStream.

        """
        from stream_graph.stream_graph import StreamGraph
        nsm = self.basic_nodestream
        return StreamGraph(self.nodeset, nsm.timeset, nsm, self)

    @property
    def as_stream_graph_minimal(self):
        """Generate the minimal stream graph containing this NodeStream.
        
        Parameters
        ----------
        None. Property.
        
        Returns
        -------
        stream_graph : StreamGraph
            Generate the StreamGraph containing the minimal NodeStream and the minimal TimeSet.

        """
        from stream_graph.stream_graph import StreamGraph
        return StreamGraph(self.nodeset, self.timeset, self.minimal_nodestream, self)
    
    # Python2 cross-compatibility
    def __nonzero__(self):
        return self.__bool__()
