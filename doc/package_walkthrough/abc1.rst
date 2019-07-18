What are the abstract base classes
==================================

In order to have a user-consistent interface irrelevant from the implementations of our base-objects, we wanted to define a set of classes with their associated, methods which correspond to all the objects needed for building a :code:`StreamGraph`.

Such objects are the :code:`NodeSet`, the :code:`LinkSet`, the :code:`TimeSet`, the :code:`TemporalNodeSet` and the :code:`TemporalLinkSet`.

Subject of this section is a walkthrough of all those classes and their methods as well as their properties. This section should be considered as a supplementary to the one of the API documentation and as so a bit less technical and more descriptive.

:code:`NodeSet`
---------------
The :code:`NodeSet`: :code:`ns` corresponds to a set of nodes.

Its :code:`ns.size` is the length of this set and boolean value :code:`bool(ns)` is True when it contains more than one element. When iterated :code:`iter(ns)` (or :code:`for n in ns`) or this object returns an unordered stream of distinct nodes.
The *containement* statement :code:`u in ns`, should be true if :code:`u` is a valid node tha belongs to the set.

As all the following classes it is defined for binary operators :code:`&`, :code:`|`, :code:`-`, corresponding to `intersection`, `union` and `difference`, between objects instantiated on the same ABC, following in this case the corresponding set operations.
Similarly the :code:`ns.issuperset(nsp)` is :code:`True` if :code:`nsp` is an instantiation of the same ABC and it is all contained inside :code:`ns`.

All the above section stands respectively for all objects and as so if we don't have anything special to mention, it will be omitted. All of those functions will be refered collectively as the *set binary operators*.
Based on its iterator, all objects have a string representation, on which they are represented as a table.
Finally a copy (with an argument of :copy:`deep=True`) is in the definition of all ABCs.

:code:`LinkSet`
---------------
The :code:`LinkSet`: :code:`ls` corresponds to a set of links.

Those links can be weighted, something which is signified by a boolean property of the class, as :code:`ls.weighted`.
Along comes the property of :code:`ls.weighted_size` which is equal with the sum of weights for each link inside the link-set.
When iterating this object, weights will appear last inside the link tuple, in case the link-set is weighted.

Certain methods of the :code:`LinkSet` are the methods :code:`ls.neighbors`, with two optional arguments :code:`u` and :code:`direction`.
If :code:`u` is not :code:`None`, the method returns all the neighbors of :code:`u` inside the link-set as a :code:`NodeSet`. Inversely if :code:`u` is :code:`None`, the method returns for each node inside the link-set its neighboring node-set, as a :code:`NodeCollection` of :code:`NodeSet` (for learning about the package collections please read the next section).
Direction is an argument given to most objects and their methods that consider links. The data is always considered to be directed. As so this argument gives the opportunity of getting the 'in', 'out' or 'both' neighbors of the graph. If the graph uni-directed (containing both entries), the direction argument could be ommiteed, as its default ("out") will reveal the only possible form of neighbors. Similar is the degree that calculates the amount of neighbors with arguments :code:`u`, :code:`direction` and final argument :code:`weights` which designates if the weighted-degree should be calculated, if the link-set is weighted.

:code:`TimeSet`
---------------
The :code:`TimeSet`: :code:`ts`, corresponds to a set of time elements.

Time-elements can be both of discrete or instantaneous (time), defined by the properties :code:`ts.discrete`, :code:`ts.instantaneous`. For separating instantaneous and non-instantaneous :code:`TimeSet` a new ABC, is defined by the name :code:`ITimeSet` (having an inheritance relation to the previous), in which case :code:`ts.instantaneous` returns aways :code:`True`, whereas in the first always :code:`False`.
This is true for all *temporal* objects, as the :code:`TemporalNodeSet` and the :code:`TemporalLinkSet`.

In both ABCs, :code:`ls.size` equals the finish minus the start of its time element, in the continuous case, where this equals the finish minus the start plus one in the continuous. For instantaneous objects, the supplementary property of :code:`ts.number_of_instants`, counts the number of distinct time-elements.

Finally a method :code:`discretize`, given some :code:`bins` and a :code:`bin_size`, should be able to convert a continuous object to a discrete one, returning the object and the associated bins.


:code:`TemporalNodeSet`
-----------------------
The :code:`TemporalNodeSet`: :code:`tns`, comes again in two flavors the normal and an instantaneous one :code:ITemporaNodeSet`, which can be decomposed to the :code:`tns.nodeset` that contains all nodes appearing in :code:`tns` and the :code:`tns.timeset`, which contains all the time-elements appearing inside the temporal-node-set. When iterated this object returns a tuple where the first element is the node and the other are the time-elements.

Size in this case is the sum: :math:`\sum_{u} |T_{u}|`, where :math:`T_{u}` is the time-set associated with the node :math:`u`.
:code:`tns.n` a parameter from graph theory stands therefore to indicate the number of distinct nodes, whereas :code:`tns.total_time` stands to indicate the amount of time the temporal-node-set exists.

Similarly for each node the :code:`tns.node_duration`: :math:`|T_{u}|` can be calculated for a certain node :code:`u` or a :code:`NodeCollection` can be returned calculating that for all nodes. Similarly :code:`tns.times_of` returns all the time-sets corresponding to each node.

The property :code:`tns.total_common_time` and the measures :code:`tns.common_time` and :code:`commont_time_pair`, calculate respectively the size of time elements appear between all node and all the other, one node and all the others and a pair of nodes. If ommited in the last two cases (which node and which pair) and :code:`NodeCollection` and a :code:`LinkCollection` are returned respectively.

Finally methods, as :code:`ls.nodes_at` or :code:`ls.n_at` (methods that monitor the evolution of some derived information inside time usually end with an :code:`_at`, whereas method that examine information related with nodes or link, throughtout time end with an :code:`_of`)  extract the NodeSet or the number of nodes active at a certain time instants or throught time, as TimeGenrator and a TimeCollection.

:code:`TemporalLinkSet`
-----------------------
The :code:`TemporalLinkSet`: :code:`tls`, comes to combine all elements discussed before, comming also into an instantaneous flavor :code:ITemporaLinkSet` and being characterized by all previous mentioned ABC properties, :code:`tls.discrete`, :code:`tls.instantaneous`, :code:`tls.weighted`.

The size of temporal-link-set is equal to the sum: :math:`\sum_{uv} |T_{uv}|` of all time-elements. As links can have weights, the temporal-link-set has a :code:`tls.weighted_size` method equal to the sum: :math:`\sum_{uv} w_{uv}|T_{uv}|`, the of all time-elements with their weights. Also for the instantaneous, the :code:`tls.number_of_instants` and the :code:`tls.weighted_number_of_instants`, can be extracted, where in the second case its instant counts as much as the sum of its weights.

When iterated, this object returns a tuple where the first two elements correspond to the link's nodes, the last two to the weights if the exists and the in-between to the time-elements.
It can be decomposed to the :code:`tls.nodeset` that contains all the nodes appearing inside the temporal-link-set, the :code:`tls.linkset` that contains all the appearing links (whose size can be extracted as :code:`tls.m`), a :code:`tns.timeset`, which contains all the appearing time-elements and two types of :code:`TemporalNodeSet`, which can be infered from it known as the :code:`ls.basic_temporal_nodeset` and the :code:`ls.minimal_temporal_nodeset`. In the first one, all nodes that exist inside the temporal-link-set exist from the begining till the end of time, whereas in the second each node-set exists, only for the time-elements that it appears inside a link.



We can extract all the links through time, as the :code:`ls.links_at` method, as a link-set (for a certain link) or a time-collection of link-sets. Similarly whe can extract all the :code:`ls.neighbors_at` of a node at a particular time-instant. If :code:`t` is ommited, we have a TimeGenerator, whereas if code:`u` is ommited we have a NodeCollection (in case both are ommited we have a NodeCollection of TimeCollection).
Similarly with a temporal-node-set, we can extract the time-set related to a particular link :code:`ls.times_of` or the time-sets of all links if the :code:`LinkCollection` is ommited.
In order to extract the TemporalNodeSet connected with a node, we have the method :code:`neihbors_of`.
As the measures of the above methods, we can find :code:`tls.m_at`, :code:`tls.degree_at`, :code:`tls.duration_of`, :code:`tls.degree_of` where all take the argument :code:`weights` which signifies if their results will be weighted or not (in case the temporal-link-set is weighted). 

Three methods exists so that a smaller part of the temporal-link-set is extracted. The most general one is the :code:`tls.filter` method which keeps only those links that satisfy a given boolean function. The next is the :code:`tls.substream` method extracts a temporal-link-set, by constraining it to at least one node-set for the left nodes of a link, to a node-set to the right nodes of a link and to time-set for the valid time-elements of a link. Finally the method :code:`tls.induced_substream` extracts the temporal-link-set that exists inside a given temporal-node-set.

Also, given a temporal-node-set we can extract the :code:`tls.temporal_neighborhood` of the temporal-link-set, as a temporal-node-set, that is the temporal-nodes that are the neighbors of some given temporal-nodes, inside the temporal-link-set.
Finally, as defined in the introduction we can extract the mamxima:code:`tls.get_maximal_cliques` inside
