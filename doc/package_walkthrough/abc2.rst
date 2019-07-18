Classes implemented on top of ABC
==================================
In this section we will examine some implementations that are based on top of ABCs.

A static Graph
--------------
Given a nodeset and a linkset one can define, an elementary :code:`Graph`, :code:`g` as the one found in classic graph theory.
This Graph can optionally be weighted and its components can be accessed through the attributes: :code:`sg.nodeset`, :code:`sg.linkset` (as these attributes return copies, if you don't want a copy of the data add a "_" to the end).
The number of nodes of the Graph, can be accessed through :code:`g.n`, where the number of edges of the Graph, can be accessed through :code:`g.m` (:code:`g.m_weighted` gives access to the weighted number of edges).
The classic density of a graph, can be refered in this context as coverage. As so by accessing the attributes :code:`g.total_coverage` and :code:`g.total_coverage_weighted` the user can calculate the ratio :math:`\\frac{m}{n^{2}}` and :math:`\\frac{m_{w}}{n^{2}}`. Moreover neighbor density per-node can be calculated as :code:`g.neighbor_coverage`, with the option of returning it for all the nodes as a :code:`NodeCollection`, if a node is ommited, as well as a :code:`direction` for :code:`'in'`, :code:`'out'` or :code:`'both'` and an argument for :code:`weights`.
Finally in order to be possible to apply more complicated measures on the graph, you can convert it to a `networkx` graph by executing: :code:`g.to_networkx`.

The Stream Graph
----------------
The most import class of the package, that finds its standard and only implementation relying only on ABCs is that of the :code:`StreamGraph`.
This class is meant to properly define a Stream Graph as defined in the introduction, as a collection of a node-set, a time-set, a temporal-node-set and a temporal-link-set.

The stream-graph: :code:`sg` is characterized by two attributes :code:`sg.n` and :code:`sg.m` as in normal graph theory, standing for the total-number of nodes and the total-number of edges, defined as the duration of all nodes inside the temporal-node-set divided by the duration of the timeset and respectively by the duration of all links inside the temporal-link-set divided by the duration of the timeset.
All methods that were defined on a single ABC, can be accessed through the attributes: :code:`sg.nodeset`, :code:`sg.timeset`, :code:`sg.temporal_nodeset`, :code:`sg.temporal_linkset` (as these attributes return copies, if you don't want a copy of the data add a "_" to the end).

Moreover measures that combine information of different ABCs are those of coverage.
When calculating :code:`ls.temporal_nodeset_coverage`, we calculate what percentage that the temporal-node-set duration covers total possible duration this set would have if all the nodes existed for all the time defined from the time-set: :math:`\frac{|W|}{|V\times T|}`. Similarly the :code:`ls.temporal_linkset_coverage` evaluates the total-duration the links exist, compared to the duration that the nodes that consitute these links exist, as :math:`\frac{|E|}{\sum_{uv \in V\\times V}|T_{u} \cap T_{v}|}` (:code:`ls.weighted_temporal_linkset_coverage` stands for the weighted case). In the same logic, we can calculate the :code:`ls.time_coverage_node` of a certain node (or the NodeCollection of it for all the nodes inside the nodeset) as ratio of its duration to the duration of the time-set: :math:`\frac{|T_{u}|}{|T|}`, as well as the :code:`ls.time_coverage_link` of a certain link (or the LinkCollection of it for all the links inside the nodeset) as a ratio of its duration to the duration  of its two consisting nodes inside the temporal-node-set: :math:`\frac{|T_{uv}|}{|T_{u} \cap T_{v}|}`. Also by calculating :code:`ls.neighbor_coverage`, we can calculate the durations of all temporal-neighbors of a node as :math:`\\frac{|N(u)|}{\\sum_{v\\in V}{|T_{u}\\cap T_{v}|}}` (a generalization of neighbor-desnity in classical graphs).

At any point in time the stream-graph can be represented by what is a called a `snapshot graph`: To access this graph, or to trace it throughout time what you can do is call the method :code:`sg.graph_at`.
Next a notion of coverage of the graph time-stamp at a certain point in time (or throughout time as a :code:`TimeCollection`), can be calculated as :code:`ls.node_coverage_at`, :code:`ls.link_coverage_at`, as well as :code:`ls.neighbor_coverage_at`, as well as its :code:`ls.mean_degree_at`, i.e. the number of links of the snapshot graph divided by the number of neighbors :math:`\frac{|E_{t}|}{|V_{t}|}`.
Any of the above methods that is related with links, accepts a :code:`weights` argument, for calculating a weighted result (for the nominator).

The binary operators of :code:`&, |, -` as well as the :code:`issuperset` are defined here as the collective result of all of these operators on the lower objects.
Additionaly the method :code:`induced_substream` of the temporal-link-set is advanced as a method of the :code:`stream-graph`, by applying an intersection, to the temporal-node-set also.
Similarly we can discretize the whole stream-graph, through the method :code:`sg.descritize`.
To conclude, a stream-graph can be also constructed through a temporal-link-set :code:`ls`, through the methods :code:`ls.as_stream_graph_basic` and :code:`ls.as_stream_graph_minimal` according to the type of the temporal-node-set derived from the temporal-link-set.

TemporalNodeSetB
----------------
This mixed class can combine a time-set ABC and a nodeset ABC to 