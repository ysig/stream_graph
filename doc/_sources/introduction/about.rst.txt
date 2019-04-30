General Information
===================

About
-----

This library is an attempt for the modelization of Stream Graphs. A
Stream Graph is a Graph whose nodes and edges (also called *links*) can
appear and disappear throughout time. Inside this module various methods
that facilitate the study of such graphs can be found, both simple as
degree distibutions (over time), as well as complicated like computing
maximal cliques, centrality-scores etc. This library is hence designed
for the analysis of the temporal dynamics of networks, such as
communication dynamics in social media.

A Stream-Graph was first formally defined by Matthieu Latapy et al. in
[`1 <https://hal.archives-ouvertes.fr/hal-01665084>`__\ ], as a object
constituted of four components: a) a set of nodes belonging to the
graph, b) a time-set representing the graph's lifespan, c) a
temporal-node-set, that is a set of nodes and times representing the
presence of node, and d) a temporal-link-set, that is a set of node
pairs and times representing the presence of link.

Authors
-------

This package has been developed by researchers of the `Complex
Networks <http://www.complexnetworks.fr/>`__ team, within the `Computer
Science Laboratory of Paris 6 <https://www.lip6.fr/>`__, for the
`ODYCCEUS <https://www.odycceus.eu/>`__ project, founded by the
`European Commission FETPROACT 2016-2017 program <https://ec.europa.eu/research/participants/portal/desktop/en/opportunities/h2020/calls/h2020-fetproact-2016-2017.html>`__
under grant 732942.

License
-------

Copyright © 2019 `Complex Networks - LIP6 <http://www.complexnetworks.fr>`__

``stream_graph`` is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. It is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GN General Public License for more details. You should have received a copy of the GNU General Public License along with this program. If not, see http://www.gnu.org/licenses/.
