import copy
from stream_graph import API
from stream_graph import NodeSetS
from stream_graph import LinkSetDF

class Graph(object):
    def __init__(self, nodeset=None, linkset=None):
        if nodeset is not None and linkset is not None:
            if isinstance(nodeset, API.NodeSet):
                self.nodeset_ = nodeset
            else:
                self.nodeset_ = NodeSetS(nodeset)
            if isinstance(linkset, API.LinkSet):
                self.linkset_ = linkset
            else:
                self.linkset_ = LinkSetDF(linkset)

    def __bool__(self):
        return hasattr(self, 'nodeset_') and hasattr(self, 'linkset_') and bool(self.nodeset_) and bool(self.linkset_)

    @property
    def nodeset(self):
        if hasattr(self, 'nodeset_'):
            return self.nodeset_.copy()
        else:
            return NodeSetS()

    @property
    def linkset(self):
        if hasattr(self, 'linkset_'):
            return self.linkset_.copy()
        else:
            return LinkSetDF()

    @property
    def n(self):
        return self.nodeset_.size
    
    @property
    def m(self):
        return self.linkset_.size

    def coverage(self, u=None, direction='out'):
        if bool(self):
            if u is None:
                return self.m/float(self.n ** 2)
            else:
                return self.linkset_.degree(u, direction=direction)/float(self.n ** 2)
        else:
            return 0.

    def copy(self, deep=True):
        if deep:
            return copy.deepcopy(self)
        else:
            return copy.copy(self)
