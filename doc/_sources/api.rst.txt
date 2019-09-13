.. _api_ref:

=============
ABC Reference
=============

.. currentmodule:: stream_graph.ABC

.. autosummary::
   :toctree: generated/
   :template: api.rst

   NodeSet
   LinkSet

.. autosummary::
   :toctree: generated/
   :template: api.rst

   TimeSet
   TemporalNodeSet
   TemporalLinkSet


.. autosummary::
   :toctree: generated/
   :template: api.rst

   ITimeSet
   ITemporalNodeSet
   ITemporalLinkSet

============
Base Classes
============

.. currentmodule:: stream_graph

.. autosummary::
   :toctree: generated/
   :template: class.rst

   Graph
   StreamGraph

===================
ABC Implementations
===================


.. currentmodule:: stream_graph

NodeSet
-------

.. autosummary::
   :toctree: generated/
   :template: api.rst

   NodeSetS
   
LinkSet
-------

.. autosummary::
   :toctree: generated/
   :template: api.rst

   LinkSetDF

TimeSet
-------

.. autosummary::
   :toctree: generated/
   :template: api.rst

   TimeSetDF


TemporalNodeSet
---------------

.. autosummary::
   :toctree: generated/
   :template: api.rst

   TemporalNodeSetDF
   TemporalNodeSetB


TemporalLinkSet
---------------

.. autosummary::
   :toctree: generated/
   :template: api.rst

   TemporalLinkSetDF

ITimeSet
--------

.. autosummary::
   :toctree: generated/
   :template: api.rst

   ITimeSetS


ITemporalNodeSet
----------------

.. autosummary::
   :toctree: generated/
   :template: api.rst

   ITemporalNodeSetDF


ITemporalLinkSet
----------------

.. autosummary::
   :toctree: generated/
   :template: api.rst

   ITemporalLinkSetDF

===================
Temporal-DataFrames
===================


.. currentmodule:: stream_graph.base.dataframes

.. autosummary::
   :toctree: generated/
   :template: class.rst

   CIntervalDF
   DIntervalDF
   CIntervalWDF
   DIntervalWDF
   InstantaneousDF
   InstantaneousWDF


==========
Visualizer
==========

.. currentmodule:: stream_graph

.. autosummary::
   :toctree: generated/
   :template: api.rst

   Visualizer


==========
Exceptions
==========

.. currentmodule:: stream_graph.exceptions

.. autosummary::
   :toctree: generated/
   :template: api.rst

   UnrecognizedNodeSet
   UnrecognizedLinkSet
   UnrecognizedTimeSet
   UnrecognizedTemporalNodeSet
   UnrecognizedTemporalLinkSet
   UnrecognizedStreamGraph
   UnrecognizedDirection
