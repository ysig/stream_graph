"""Exceptions of `stream_graph` module."""


class UnrecognizedNodeSet(ValueError):
    def __init__(self, variable_name, extra_message=""):
        ValueError.__init__(self, variable_name + " should be an LinkSet instance\n" + extra_message)


class UnrecognizedLinkSet(ValueError):
    def __init__(self, variable_name, extra_message=""):
        ValueError.__init__(self, variable_name + " should be an LinkSet instance\n" + extra_message)


class UnrecognizedTimeSet(ValueError):
    def __init__(self, variable_name, extra_message=""):
        ValueError.__init__(self, variable_name + " should be an TimeSet instance\n" + extra_message)


class UnrecognizedNodeStream(ValueError):
    def __init__(self, variable_name, extra_message=""):
        ValueError.__init__(self, variable_name + " should be an NodeStream instance\n" + extra_message)


class UnrecognizedLinkStream(ValueError):
    def __init__(self, variable_name, extra_message=""):
        ValueError.__init__(self, variable_name + " should be an NodeStream instance\n" + extra_message)


class UnrecognizedDirection(ValueError):
    def __init__(self):
        ValueError.__init__(self, "Unrecognised direction (valid directions are 'in', 'out', 'both')")
