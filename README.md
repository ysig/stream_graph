

# A Python Library for the Analysis of Stream Graphs

This library is an attempt at modelling Stream Graphs.
A Stream Graph is a graph which nodes and links appear and disappear through time.
Various methods that facilitate the study of such graphs can be found in this library, both simple (as degree distribution over time) and sophisticated (as maximal temporal-cliques, temporal-centrality measures).
This library is hence designed for the analysis of the temporal dimension of evolving networks, such as the communication dynamics in social media. 

Stream Graphs were first formally defined by [Matthieu Latapy *et al.*](https://hal.archives-ouvertes.fr/hal-01665084) as the generalization of static graphs.
They consist of four components: (1) a set of nodes (`NodeSet`) belonging to the graph, (2) a time interval (`TimeSet`) representing the graph's lifespan, (3) a set of temporal nodes (`TemporalNodeSet`) describing instants when nodes are present in the stream, and (4) a set of temporal links (`TemporalLinkSet`) describing the instants when nodes are interacting in the stream.

**Warning:** This library is currently **under development**. Elementary structures and methods may change, without guarantying the support for previous versions.


## Installing the Library

Update: Version `0.2`
---------------------
- Changes in interval-dataframe backbone:
  - Continuous (time) intervals.
  - Discrete (time) Intervals are treated differently
- TODO: More Verification (add continuous bounds for maximal_cliques)


Installing `stream_graph`
-------------------------
The `stream_graph` library requires:

* Python [>=2.7, >=3.5]
* Numpy [>=1.14.0]
* Pandas [>=0.24.0]
* Cython [>=0.27.3]
* six [>=1.11.0]
* svgwrite [>=1.2.1]


### Installing Dependencies


To install dependencies:

```shell
pip install extension>=extension_version
```

Or more simply:

```shell
pip install -r requirements.txt
```
Please add `sudo` if `pip` does not have superuser privileges.


### Installing the Master Version


```shell
pip install git+https://github.com/ysig/stream_graph/
```


## Getting Started


For a first introduction to the library, please have a look at [emailEU](https://nbviewer.jupyter.org/github/ysig/stream_graph/blob/master/tutorials/emailEU/email-Eu.ipynb) or visit the [tutorials page](https://ysig.github.io/stream_graph/doc/tutorials.html) within the documentation.


## Documentation


The [library documentation](https://ysig.github.io/stream_graph/doc/) is available online and automatically generated with Sphinx.
To generate it yourself, move to `doc` folder and execute: `make clean hmtl`, after having installed all the needed dependencies.

## Authors

This package has been developed by researchers of the [Complex Networks](http://www.complexnetworks.fr/) team, within the [Computer Science Laboratory of Paris 6](https://www.lip6.fr/), for the [ODYCCEUS](https://www.odycceus.eu/) project, founded by the [European Commission FETPROACT 2016-2017 program](https://ec.europa.eu/research/participants/portal/desktop/en/opportunities/h2020/calls/h2020-fetproact-2016-2017.html) under grant 732942.

### Contact
* Yiannis Siglidis: <Yiannis.Siglidis@lip6.fr>
* Robin Lamarche-Perrin: <Robin.Lamarche-Perrin@lip6.fr>

## License

Copyright © 2019 [Complex Networks - LIP6](<http://www.complexnetworks.fr>)

`stream_graph` is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. It is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GN  General Public License for more details. You should have received a copy of the GNU General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.
