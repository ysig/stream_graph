Elementary Definitions
======================

In this section we will walk through some elementary definitions of derived properties or **measures** in a Graphs and proceed to their temporal-generalizations, inside the theoretical framework of Graphs.

Number of Nodes
----------------
In a classical graph :math:`G`, the *number of nodes* 

.. math::

    n_{G} = |V|

In a stream-graph :math:`S`, this is generalized by defining the *number of nodes* to be:

.. math::

    n_{S} = \sum_{u\in V}n_{u} = \sum_{u\in V}\frac{|T_{u}|}{|T|} = \frac{|W|}{|T|}


where :math:`n_{u} \leq 1` is called the **contribution** of a node and is equal to :math:`1` **iff** a node exists during the the whole timeset.

As :math:`W \subseteq V \times T`, :math:`n_{S} = \frac{|W|}{|T|} \leq |V|` and equality exists **iff** all nodes appear during all the time, defined by the timeset :math:`T`. 

Coverage
--------
A very simple measure of the stream-graph, that can show us in what amount our temporal-node-set is dynamic, is called
the **coverage**:

.. math::

    c(S)=\frac{|W|}{|V\times T|}


As :math:`W \subseteq V \times T`, this measure will always be less or equal to :math:`1` and equal **iff** all nodes appear for all the time, that is defined by the timeset :math:`T`.  
Notice that: 

.. math::

    c(S) = \frac{|W|}{|V| |T|} = \frac{\frac{|W|}{|T|}}{|V|} = \frac{n_{S}}{|V|}


So as this ratio goes to 1, the nodes of the graph, can more be conceived as being **static**.


Number of edges
---------------
In a classical graph :math:`G`, the number of edges:

.. math::

    m_{G} = |E|


In a stream-graph :math:`S`, this is generalized by defining the number of links to be: 

.. math::

    m_{S} = \sum_{u,v \in V}m_{uv} = \sum_{u,v\in V}\frac{|T_{uv}|}{|T|} = \frac{|Z|}{|T|}

where :math:`m_{uv} \leq 1` is called the **contribution** of a link and is equal to :math:`1` if the link exists during the the whole timeset.


Density
-------
In a classical graph :math:`G`, the *density* is defined as:

.. math::

    \delta(G) = \frac{|E|}{|V|(|V|-1)}


In a stream-graph :math:`S`, this is generalized by defining the density as:

.. math::

    \delta(S) = \frac{\sum_{uv \in V}|T_{uv}|}{\sum_{uv \in V}|T_{u} \cap T_{v}|} = \frac{|Z|}{\sum_{uv \in V}|T_{u} \cap T_{v}|}

Similar to that we can define the *density* of a **node** as: 

.. math::

    \delta(v) = \frac{\sum_{u \in V, u\neq v}|T_{uv}|}{\sum_{u \in V, u\neq v}|T_{u} \cap T_{v}|}


and the *density* of a **link** as:


.. math::

    \delta(uv) = \frac{|T_{uv}|}{|T_{u} \cap T_{v}|}

Note that in all of the above measures if the denominator is **zero**, the measure is defined as being **zero**.
