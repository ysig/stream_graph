from __future__ import absolute_import
import copy
import abc

from .utils import ABC_to_string

# 2/3 Cross Compatibility
ABC = abc.ABCMeta('ABC', (object,), {'__slots__': ()}) 

class NodeSet(ABC):
    """NodeSet Object API Specification.
    
    A NodeSet can be abstractly be defined as a set of node :code:`u`.

    """

    def __str__(self):
        return ABC_to_string(self, columns=['u'])

    @property
    @abc.abstractmethod
    def size(self):
        """Returns the size of the NodeSet.

        Parameters
        ----------
        None. Property.
        

        Returns
        -------
        size : Real
            The number of node.
        
        """
        pass
    
    @abc.abstractmethod
    def __contains__(self, n):
        """Implementation of the :code:`in` operator for NodeSet.
        
        Parameters
        ----------
        u : Node_Id

        Returns
        -------
        contains : Bool
            Returns True if the u appears inside the NodeSet.
        
        """
        pass

    @abc.abstractmethod
    def __bool__(self):
        """Implementation of the :code:`bool` casting of a NodeSet object.
        
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
        """Implementation of the :code:`&` operator for a NodeSet object.
        
        Parameters
        ----------
        ns : NodeSet
        
        Returns
        -------
        out : NodeSet
            Returns the **intersection** of Nodes.

        """
        pass
        
    @abc.abstractmethod
    def __or__(self, ns):
        """Implementation of the :code:`|` operator for a NodeSet object.
        
        Parameters
        ----------
        ns : NodeSet
        
        Returns
        -------
        out : NodeSet
            Returns the **union** of Nodes.

        """
        pass
        
    @abc.abstractmethod
    def __sub__(self, ns):
        """Implementation of the :code:`-` operator for a NodeSet object.

        Parameters
        ----------
        ns : NodeSet
        
        Returns
        -------
        out : NodeSet
            Returns the **difference** of Nodes.

        """
        pass

    @abc.abstractmethod
    def __iter__(self):
        """Implementation of the :code:`iter` function for a NodeSet object.
        
        Parameters
        ----------
        None
        
        Returns
        -------
        out : Iterator of Node_Id

        """
        pass

    @abc.abstractmethod
    def issuperset(self, ns):
        """Check if a NodeSet contains another NodeSet.
        
        Parameters
        ----------
        ns : NodeSet
        
        Returns
        -------
        issuperset_f : Bool
            True if all links of ns appear in this NodeSet.

        """
        pass
    
    def copy(self, deep=True):
        """Returns a deep or shallow copy of the current NodeSet.
        
        Parameters
        ----------
        deep : Bool
        
        Returns
        -------
        nodeset_copy : NodeSet
            Returns a deep or shallow copy of the current NodeSet.

        """
        if deep:
            return copy.deepcopy(self)
        else:
            return copy.copy(self)
