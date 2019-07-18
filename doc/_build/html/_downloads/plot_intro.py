"""
==========================================
Initialize and print a simple stream-graph
==========================================

An example plot of a :class:`stream_graph.StreamGraph` made from a combination of :class:`stream_graph.NodeSetS`, :class:`stream_graph.TimeSetDF` , :class:`stream_graph.TemporalNodeSetDF`, :class:`stream_graph.LinkStreamDF`:

.. image:: images/sphx_glr_plot_intro_001.png
   :align: center

using the :class:`stream_graph.Visualizer`.

"""
from __future__ import print_function
print(__doc__)

from stream_graph import Visualizer, TemporalLinkSetDF, NodeSetS, TemporalNodeSetDF, TimeSetDF, StreamGraph

nodeset = NodeSetS({1, 2, 3})
timeset = TimeSetDF([(0, 10)])
temporal_nodeset = TemporalNodeSetDF([(1, 1, 9), (2, 2, 5), (2, 7, 9)])
temporal_linkset = TemporalLinkSetDF([(1, 2, 2, 4), (1, 2, 3, 5), (1, 2, 6, 8)] , disjoint_intervals=False)
stream_graph = StreamGraph(nodeset, timeset, temporal_nodeset, temporal_linkset)

# Visualize svg
Visualizer(stream_graph, 'plot_intro.svg', image_type='svg').produce()

# Convert to PNG
from cairosvg import svg2png
from shutil import move
filename = '../doc/auto_examples/images/sphx_glr_plot_intro_001.png'
svg2png(url='plot_intro.svg', write_to=filename, parent_height=160, parent_width=453)

# Remove svg
from os import remove
remove('plot_intro.svg')