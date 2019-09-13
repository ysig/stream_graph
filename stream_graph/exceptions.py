"""Exceptions of `stream_graph` module."""


class UnrecognizedNodeSet(ValueError):
    """Exception raised when a variable is not an instance of ABC.NodeSet."""
    def __init__(self, variable_name, extra_message=""):
        """Initialize invalid ABC.NodeSet Exception.

        Parameters
        ----------
        variable_name : str
            The name of the variable which is not a ABC.NodeSet.

        extra_message : str, default=""
            Message to be appended after the end of the exception.

        """
        ValueError.__init__(self, variable_name + " should be a LinkSet instance\n" + extra_message)


class UnrecognizedLinkSet(ValueError):
    """Exception raised when a variable is not an instance of ABC.LinkSet."""
    def __init__(self, variable_name, extra_message=""):
        """Initialize invalid ABC.LinkSet Exception.

        Parameters
        ----------
        variable_name : str
            The name of the variable which is not a ABC.LinkSet.

        extra_message : str, default=""
            Message to be appended after the end of the exception.

        """
        ValueError.__init__(self, variable_name + " should be a LinkSet instance\n" + extra_message)


class UnrecognizedTimeSet(ValueError):
    """Exception raised when a variable is not an instance of ABC.TimeSet."""
    def __init__(self, variable_name, extra_message=""):
        """Initialize invalid ABC.TimeSet or ABC.ITimeSet Exception.

        Parameters
        ----------
        variable_name : str
            The name of the variable which is not a ABC.TimeSet or ABC.ITimeSet.

        extra_message : str, default=""
            Message to be appended after the end of the exception.

        """
        ValueError.__init__(self, variable_name + " should be a TimeSet instance\n" + extra_message)


class UnrecognizedTemporalNodeSet(ValueError):
    """Exception raised when a variable is not an instance of ABC.TemporalNodeSet."""
    def __init__(self, variable_name, extra_message=""):
        """Initialize invalid ABC.TemporalNodeSet or ABC.ITemporalNodeSet Exception.

        Parameters
        ----------
        variable_name : str
            The name of the variable which is not a ABC.TemporalNodeSet or ABC.ITemporalNodeSet.

        extra_message : str, default=""
            Message to be appended after the end of the exception.

        """
        ValueError.__init__(self, variable_name + " should be a TemporalNodeSet instance\n" + extra_message)


class UnrecognizedTemporalLinkSet(ValueError):
    """Exception raised when a variable is not an instance of ABC.TemporalLinkSet."""
    def __init__(self, variable_name, extra_message=""):
        """Initialize invalid ABC.TemporalLinkSet or ABC.ITemporalLinkSet Exception.

        Parameters
        ----------
        variable_name : str
            The name of the variable which is not a ABC.TemporalLinkSet or ABC.ITemporalLinkSet.

        extra_message : str, default=""
            Message to be appended after the end of the exception.

        """
        ValueError.__init__(self, variable_name + " should be a TemporalLinkSet instance\n" + extra_message)

class UnrecognizedStreamGraph(ValueError):
    """Exception raised when a variable is not an instance of ABC.TemporalStreamGraph."""
    def __init__(self, variable_name, extra_message=""):
        """Initialize invalid ABC.StreamGraph Exception.

        Parameters
        ----------
        variable_name : str
            The name of the variable which is not a ABC.StreamGraph.

        extra_message : str, default=""
            Message to be appended after the end of the exception.

        """
        ValueError.__init__(self, variable_name + " should be a StreamGraph\n" + extra_message)


class UnrecognizedDirection(ValueError):
    """Exception when we do not have a valid Direction."""
    def __init__(self):
        """Initialize invalid UnrecognizedDirection Exception."""
        ValueError.__init__(self, "Unrecognised direction (valid directions are 'in', 'out', 'both')")
