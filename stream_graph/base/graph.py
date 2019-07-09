from __future__ import absolute_import
import copy
from stream_graph import ABC
from .node_set_s import NodeSetS
from .link_set_df import LinkSetDF
from stream_graph.collections import NodeCollection

class Graph(object):
    """Graph
    
    A Graph :math:`S=(V, E)` is a collection of two elements:

        - :math:`V`, a node-set
        - :math:`E`, a link-set

    """
    def __init__(self, nodeset=None, linkset=None, weighted=False):
        if nodeset is not None and linkset is not None:
            if isinstance(nodeset, ABC.NodeSet):
                self.nodeset_ = nodeset
            else:
                self.nodeset_ = NodeSetS(nodeset)
            if isinstance(linkset, ABC.LinkSet):
                self.linkset_ = linkset
            else:
                self.linkset_ = LinkSetDF(linkset, weighted=weighted)

    def __bool__(self):
        return hasattr(self, 'nodeset_') and hasattr(self, 'linkset_') and bool(self.nodeset_) and bool(self.linkset_)

    # Python2 cross-compatibility
    __nonzero__ = __bool__


    def __str__(self):
        if bool(self):
            out = [('Node-Set', str(self.nodeset_))]
            out += [('Link-Set', str(self.linkset_))]
            header = ['Graph']
            header += [len(header[0])*'=']
            return '\n\n'.join(['\n'.join(header)] + ['\n'.join([a, len(a)*'-', b]) for a, b in out])
        else:
            out = ["Empty Graph"]
            out = [out[0] + "\n" + len(out[0])*'-']
            if not hasattr(self, 'nodeset_'):
                out += ['- Node-Set: None']
            elif not bool(self.nodeset_):
                out += ['- Node-Set: Empty']
            if not hasattr(self, 'linkset_'):
                out += ['- Link-Set: None']
            elif not bool(self.linkset_):
                out += ['- Link-Set: Empty']
            return '\n\n  '.join(out)

    @property
    def weighted(self):
        return self.linkset_.weighted

    @property
    def nodeset(self):
        """Extract the nodeset.
        
        Parameters
        ----------
        None. Property
        
        Returns
        -------
        nodeset: ABC.NodeSet
            Returns a copy of the nodeset defining this graph.

        """
        if hasattr(self, 'nodeset_'):
            return self.nodeset_.copy()
        else:
            return NodeSetS()

    @property
    def linkset(self):
        """Extract the linkset.
        
        Parameters
        ----------
        None. Property
        
        Returns
        -------
        linkset: ABC.LinkSet
            Returns a copy of the linkset defining this graph.

        """
        if hasattr(self, 'linkset_'):
            return self.linkset_.copy()
        else:
            return LinkSetDF()

    @property
    def n(self):
        """Extract the number of nodes.
        
        Parameters
        ----------
        None. Property.
        
        Returns
        -------
        n: Int
            Returns the size of the nodeset defining this graph.

        """
        return self.nodeset_.size
    
    @property
    def m(self):
        """Extract the number of links.
        
        Parameters
        ----------
        None. Property
        
        Returns
        -------
        m: Int
            Returns the size of the linkset defining this graph.

        """
        return self.linkset_.size

    @property
    def m_weighted(self):
        """Extract the weighted number of links.
        
        Parameters
        ----------
        None. Property
        
        Returns
        -------
        m: Int
            Returns the size of the linkset defining this graph.

        """
        return self.linkset_.size_weighted

    @property
    def total_coverage(self):
        """Extract the total coverage of the graph.
        
        Parameters
        ----------
        None. Property.
        
        Returns
        -------
        total_coverage: Real
            Returns :math:`\\frac{m}{n^{2}}`.

        """
        if bool(self):
            return self.m/float(self.n ** 2)
        else:
            return 0.

    @property
    def total_coverage_weighted(self):
        """Extract the weighted total coverage of the graph.
        
        Parameters
        ----------
        None. Property.
        
        Returns
        -------
        total_coverage: Real
            Returns :math:`\\frac{m}{n^{2}}`.

        """
        if bool(self):
            return self.m_weighted/float(self.n ** 2)
        else:
            return 0.

    def to_networkx(self, create_using=None):
        """Convert Graph to a networkx graph.
        
        Parameters
        ----------
        create_using : (NetworkX graph constructor, optional (default=nx.Graph))
            Graph type to create. If graph instance, then cleared before populated.
        
        Returns
        -------
        graph : nx.Graph
        """
        import networkx as nx
        if create_using is None:
            G = nx.Graph()
        else:
            assert isinstance(create_using, nx.Graph)
            G = create_using()

        G.add_nodes_from(self.nodeset_)
        if linkset_.weighted:
            G.add_weighted_edges_from(self.linkset_)
        else:
            G.add_edges_from(self.linkset_)    
    
        return G

    def coverage(self, u=None, direction='out', weights=False):
        """Extract the total coverage of the graph.
        
        Parameters
        ----------
        u: NodeId or None

        direction: 'in', 'out' or 'both', default='out'

        weights: Bool

        Returns
        -------
        total_coverage: Real or NodeCollection
            If u is Real, returns :math:`\\frac{d_{direction}(u)}{n^{2}}`.
            Otherwise returns the coverage of each node.

        """
        if bool(self):
            denom = float(self.n ** 2)
            if u is None:
                def fun(x, y):
                    return y/denom
                return self.linkset_.degree(direction=direction, weights=weights).map(fun)
            else:
                return self.linkset_.degree(u, direction=direction, weights=weights)/denom
        else:
            if u is None:
                return NodeCollection()
            else:
                return 0.

    def copy(self, deep=True):
        if deep:
            return copy.deepcopy(self)
        else:
            return copy.copy(self)
