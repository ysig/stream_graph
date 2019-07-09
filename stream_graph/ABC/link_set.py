from __future__ import absolute_import
import copy
import abc

from .utils import ABC_to_string

# 2/3 Cross Compatibility
ABC = abc.ABCMeta('ABC', (object,), {'__slots__': ()})

class LinkSet(ABC):
    """LinkSet Object API Specification.

    A LinkSet can be abstractly be defined as a set of links :code:`(u, v)`.

    """

    def __str__(self):
        return ABC_to_string(self, ['u', 'v'])

    @property
    @abc.abstractmethod
    def weighted(self):
        """Designate if the LinkSet has weights.

        Parameters
        ----------
        None. Property.


        Returns
        -------
        weights : Bool
            True if the links are weighted.
            None if the linkset is empty.

        """
        pass

    @property
    def size(self):
        """Returns the size of the LinkSet.

        Parameters
        ----------
        None. Property.


        Returns
        -------
        size : Real
            The number of links.

        """
        return sum(1 for _ in iter(self))

    @property
    def weighted_size(self):
        """Returns the weighted size of the LinkSet.

        Parameters
        ----------
        None. Property.


        Returns
        -------
        size : Real
            The sum of weights of links.

        """
        weighted = self.weighted
        if bool(self):
            if weighted:
                return self._weighted_size
            else:
                return self.mdf.shape[0]
        else:
            return .0

    def _weighted_size(self):
        return sum(k[-1] for k in iter(self))


    @abc.abstractmethod
    def __bool__(self):
        """Implementation of the :code:`bool` casting of a LinkSet object.

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
    def neighbors(self, u=None, direction='out'):
        """Return the nodeset of a neighbors of a node.

        Parameters
        ----------
        u : Node_Id or None

        direction : string={'in', 'out', 'both'}, default='both'

        Returns
        -------
        nodestream : NodeSet or NodeCollection(NodeSet)
            Return the ('in', 'out' or 'both') nodeset of neighbors of u.
            If u is None, returns a NodeCollection of its node with its neighbors.

        """
        pass

    def degree(self, u=None, direction='out', weights=False):
        """Return the degree of a node.

        Parameters
        ----------
        u : Node_Id, default=None

        direction : string={'in', 'out', 'both'}, default='both'

        weights : bool, default=False

        Returns
        -------
        nodestream : Real or NodeCollection(Real)
            Return the ('in', 'out' or 'both') degree of a node of u.
            If None returns a NodeCollection with the degree for each Node.
.
        """
        if weights and self.weighted:
            return self._degree_weighted(u, direction)
        else:
            return self._degree_unweighted(u, direction)

    @abc.abstractmethod
    def _degree_weighted(self, u, direction):
        pass

    @abc.abstractmethod
    def _degree_unweighted(self, u, direction):
        pass

    @abc.abstractmethod
    def __contains__(self, l):
        """Implementation of the :code:`in` operator for LinkSet.

        Parameters
        ----------
        l : tuple, len(l) == 2
            l[0] : Node_Id or None
            l[1] : Node_Id or None

        Returns
        -------
        contains : Bool
            Returns true if the (u, v) appears in the LinkSet.
            If u is None v can match with anything and if v is None the opposite.
            If both u and v is None this function should return False.

        """
        pass

    @abc.abstractmethod
    def __and__(self, ls):
        """Implementation of the :code:`&` operator for a LinkSet object.

        Parameters
        ----------
        ls : LinkSet

        Returns
        -------
        out : LinkSet
            Returns the **intersection** of Links.

        """
        pass

    @abc.abstractmethod
    def __or__(self, ls):
        """Implementation of the :code:`|` operator for a LinkSet object.

        Parameters
        ----------
        ls : LinkSet

        Returns
        -------
        out : LinkSet
            Returns the **union** of Links.

        """
        pass

    @abc.abstractmethod
    def __sub__(self, ls):
        """Implementation of the :code:`-` operator for a LinkSet object.

        Parameters
        ----------
        ls : LinkSet

        Returns
        -------
        out : LinkStream
            Returns the **difference** of Links.

        """
        pass

    @abc.abstractmethod
    def __iter__(self):
        """Implementation of the :code:`iter` function for a LinkSet object.

        Parameters
        ----------
        None

        Returns
        -------
        out : Iterator of tuple
            Each tuple is of the form (u, v)

        """
        pass

    @abc.abstractmethod
    def issuperset(self, ls):
        """Check if a LinkSet contains another LinkSet.

        Parameters
        ----------
        ls : LinkSet

        Returns
        -------
        issuperset_f : Bool
            True if all links of ls appears in this LinkSet.

        """
        pass

    def copy(self, deep=True):
        """Returns a deep or shallow copy of the current LinkSet.

        Parameters
        ----------
        deep : Bool

        Returns
        -------
        linkset_copy : LinkSet
            Returns a deep or shallow copy of the current LinkSet

        """
        if deep:
            return copy.deepcopy(self)
        else:
            return copy.copy(self)
