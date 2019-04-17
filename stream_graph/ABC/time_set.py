import copy
import abc
from warnings import warn

from ._utils import ABC_to_string

# 2/3 Cross Compatibility
ABC = abc.ABCMeta('ABC', (object,), {'__slots__': ()}) 

class TimeSet(ABC):
    """TimeSet Object API Specification.
    
    A TimeSet can be abstractly be defined as a set of intervals :code:`(ts, tf)`.

    """

    def __str__(self):
        return ABC_to_string(self)

    @property
    def instantaneous(self):
        """Defines if the Time Set is instantaneous."""
        return False

    @property
    @abc.abstractmethod
    def discrete(self):
        """Designate if the TimeSet is on discrete Time.
        
        Parameters
        ----------
        None. Property.
        

        Returns
        -------
        discrete : Bool
            True if the time is discrete.
            Returns None if empty.

        """
        pass

    @property
    @abc.abstractmethod
    def size(self):
        """Returns the size of the TimeSet.
        
        Parameters
        ----------
        None. Property.
        

        Returns
        -------
        size : Real
            The total amount of time inside the TimeSet.
        
        """
        pass

    @abc.abstractmethod
    def __contains__(self, t):
        """Implementation of the :code:`in` operator for TimeSet.
        
        Parameters
        ----------
        l : tuple or Real
            If tuple: :code:`(ts, tf)`. If Real :code:`t`.

        Returns
        -------
        contains : Bool
            Returns true if time :code:`t` the :code:`(ts, tf)` is contained inside the :code:`TimeSet`.
        
        """
        pass

    @abc.abstractmethod
    def __and__(self, ts):
        """Implementation of the :code:`&` operator for a TimeSet object.
        
        Parameters
        ----------
        ns : TimeSet
        
        Returns
        -------
        out : TimeSet
            Returns the **intersection** of intervals.

        """
        pass

    @abc.abstractmethod
    def __or__(self, ts):
        """Implementation of the :code:`|` operator for a TimeSet object.
        
        Parameters
        ----------
        ns : TimeSet
        
        Returns
        -------
        out : TimeSet
            Returns the **union** of intervals.

        """
        pass

    @abc.abstractmethod
    def __sub__(self, ts):
        """Implementation of the :code:`-` operator for a TimeSet object.
        
        Parameters
        ----------
        ns : TimeSet
        
        Returns
        -------
        out : TimeSet
            Returns the **difference** of intervals.

        """
        pass

    @abc.abstractmethod
    def __iter__(self):
        """Implementation of the :code:`iter` function for a TimeSet object.
        
        Parameters
        ----------
        None
        
        Returns
        -------
        out : Iterator of tuple
            Each tuple is of the form (ts, tf).

        """
        pass

    @abc.abstractmethod
    def __bool__(self):
        """Implementation of the :code:`bool` casting of a TimeSet object.
        
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

    def copy(self, deep=True):
        """Returns a deep or shallow copy of the current TimeSet.
        
        Parameters
        ----------
        deep : Bool
        
        Returns
        -------
        timeset_copy : TimeSet
            Returns a deep or shallow copy of the current TimeSet.

        """
        if deep:
            return copy.deepcopy(self)
        else:
            return copy.copy(self)

    def discretize(self, bins=None, bin_size=None):
        """Returns a discrete version of the current TimeSet.
        
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

class ITimeSet(TimeSet):
    """Instantaneous TimeSet Object API Specification.
    
    A TimeSet can be abstractly be defined as a set of time-stamps :code:`t`.

    """

    @property
    def instantaneous(self):
        """Defines if the Time Set is instantaneous."""
        return True
