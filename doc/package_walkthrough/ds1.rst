Choosing the optimal data-structure
====================================

Constraints for choosing the optimal data-structure
---------------------------------------------------
The first question that someone should ask when starting to implement a library, is "what is/are the elementary data-structure(s), on top of which I will define my base objects".
In order to answer to that question, you should consider a couple of constraints.

Firstly, what is the main data-type the objects suppose and how are they connected in our theoretical context. This is important both for the comprehension of the library, as well as the requirements defined by the I/O of the library, in the context of the user program.
For example our library came to support *time* as a real number (float or Integer) in order to be user-friendly, while the greatest majority of datasets it is represented as a unix-timestamp, which corresponds to an Integer.
Now concerning our objects, as defined previously each one of those is a cartesian product of vertex sets (which I will refer to as "keys"), that is distinct elements such as string or integer identifiers and a Time-Set (wich I will be refering to as "times" or "intervals"), such as a set of discrete or continuous time-stamps for instantaneous objects or discrete or continuous time-intervals. Discrete time, thus concerns natural numbers whereas continuous real-numbers. Also in order for our library to be as consistent as possible, we would like our continuous intervals to have *bounds*. Note here, that discrete intervals can be represented as Instantaneous, where this is not true for continuous intervals.
Additionally, there is the need of including and handling weights for temporal-link-set, while being able to define on them a given set of operations.

So our next constraint, is the type of operations that the data-structures should handle. All our prereferenced objects, should handle binary operations between cartesian products of sets, such as union, intersection, difference, checking if one set is the superset of the other and more, as well as allowing an iteration of the data and an extraction of a subset of them as a new object.
As our objects are interconnected, we would also want operations that map temporal-link-sets to temporal-node-sets, or temporal-node-sets to time-sets, or alternatively operations that combine such objects to be possible without a big overhead, that is a conversion from one type of data-structure to another.

In addition to that, we should take into consideration how often will each operation be used.
For example the classical question about representing your data as a binary tree is always how much times you will call operations that are executed faster in binary-trees than in sorted data. Initializing a binary tree is computationally expensive and memory consuming. As so it can be less scalable and inefficient if the number of operation that exploit this property are not significantly many.

Moreover in a bit higher level we need to envision from the standpoint of simple measures defined on stream-graphs (such as size, degree, coverage) how they could be deduced from basic methods and attributes of our data-structures and reassure that they remain simple.
Of-course this leads to considering existing implementations of simple and more advanced algorithms from shortest-paths to maximal-cliques.

As the area of research around Stream-Graphs is still not mature there is no standard implementation.
At their most general approach, stream-graphs are considered as time-ordered streams of information. This is similar with representing graphs with edge-lists.
Considering the fact that events are distributed more through time and less between vertices (huge static graph structures do not appear really often - in which case we go more to the field of dynamic graphs), allows this basic data-structure to be optimal for most approaches, calculating on the fly data, that will not be used by another operation, or such that their time-overhead is not important compaired to their memory-one.
A final constraint also goes with the programming language each implementation uses, as well as the convenience that each programming language allows. 
It was from the project's initial goal to be built in Python as it is user-friendly and allows fast components by integrating C++ code and it has been the number one, most popular language.


The optimal data-structure
--------------------------
After examining and discovering all these research questions, we started searching what could be our ideal data-structure.
One proposal was called the `interval-tree <https://en.wikipedia.org/wiki/Interval_tree>`_.
Interval-trees are trees similar to binary trees defined in order to store intervals.
Similarly with binary trees creation cost is in the order of :math:`O(n \log n)` and memory cost is in the same order as the input :math:`O(n)`.
In case of timesets, this could be considered as a reasonable approach. But in order to include all other objects that have keys, i.e. the TemporalLinkSet and the TemporalNodeSet, we would have an interval tree for each key. As so operations from the one of initializing an object, or more compplicated such as union or intersection, would be split to operations between a lot of interval-trees. For example quering something in time would mean merge the set of all queries on the interval-trees associated with each-key. Although, this was easier to formulate a **dependency on keys** was not preferable.  

Inversely, another approach could be the following. Let's say we have an interval-tree, where each interval contains all the keys that exist at that time. 
As so an binary operation, such as union between two objects would be the result of taking the union of two interval trees with the exception that if we don't have common values at common intervals an new interval-should be defined and if two intervals turn out to contain the same elements they should be merged into a common one.
In this formulation, we have a **dependency on time** and as so searching for a key means searching all the elements of the interval sets. This idea, was also closer to a different theoretical formulation, that was called dynamic graphs, which considers temporal graphs, as static graphs that change through time.

Ofcourse, by constructing both objects upon initialization we would have both dependencies covered at the same-time, but taking twice the space, the development and the operations between elementary objects. Generally, non-monolithic approaches, i.e. mixed objects, should be able to avoid this problem, by organizing the information of the unit objects in a way that the critical majority of updates of the objects information, correspond to a single unit and thus are not made twice.

Also smaller libraries already existed in Python (only for linkstreams - stream-graphs without a temporal-nodeset) and in C++ code wrapped in Python code namely the one of Leo Ranou and the one of `Noe Gaumont <https://bitbucket.org/nGaumont/liblinkstream/src/master/>`_, where the first considered a mixed approach like the one discussed before (map of interval trees and interval tree of keys and the second) for an interval-linkstream and the second an ordered-set of :code:`(u, v, t)` links and a map/view between edges and the associated links where this edges appear inside a queue of pointers, for an instantaneous link-stream.


As so, we came to the conclusion that our ideal data-structure, would be a multi-dimensional ordering of keys (distinct elements) and intervals.
The two closest such data-structures where the `R-Trees <https://en.wikipedia.org/wiki/R-tree>`_ and the `B-Trees <https://en.wikipedia.org/wiki/B-tree>`_.
An initial approach to define Stream-Graphs using `C++ Boost library R-Trees <https://www.boost.org/doc/libs/1_65_1/libs/geometry/doc/html/geometry/reference/spatial_indexes/boost__geometry__index__rtree.html>`_ failed as R-trees are spatial objects, which meant that distinct objects would be mapped into a continuous space where their distance (derived from their ordering position), would have have a meaning, although in our case it wouldn't mean anything (node 'a' and node 'b' are not closer with each other than node 'z') and as so *box-queries* in the node-space where not useful. Moreover defining more complex operations on our R-trees such as taking the temporal-neighborhood of a temporal-link-set through a temporal-node-set, prooved nearly impossible (a lot of approaches where considered: for the temporal-link-set we could have as space the :code:`u, v, t` and no-value (or a weight) or as a space the :code:`u, t` and as a value the :code:`v`).
Moreover, the idea of finding a spatial embedding i.e. of a Temporal-Link-Set in an R-tree space, was rejected as beeing in really complicated in a fundamental level.

Now, as far as B-trees are concerned the idea was derived from multi-level queries of SQL, that sometimes contain datetime and users. We later found out, that in order do such queries an index should be made on which a binary search is later applied. This index on the other hand doesn't facilitate binary operations between such objects.

At that time, we had less than nine-months to code the entire library, so designing our own data-structure from scratch didn't become our main-objective.
As a result we approached the problem in a different way. We defined our abstract base-classes, which is another name for java-interfaces in python, providing space for someone who would tackle this problem in a more efficient way in the future, to implement her own version.
Also developing and using complicated c++ libraries for the core of our package, which were not already wrapped succesfully in python packages, was not our first candidate, as interfacing with Cython, multi-machine support and user-friendly IO becomes a bit more complicated.

Why Pandas?
-----------
As a result of immaturity of the area of Stream-Graphs, the lack of standard data-structures between implementations and the fact that we decided to represent our data in the most simplistic and easy-to-handle way, we figured out that a pandas DataFrame, was good starting point.
This approach, was as similar as having an edge list for a graph.
Pandas uses numpy as a backbone, which has a very good performance in a huge variety of systems and which is already installed in almost all computers that use Python. From primary experiments it had a really good module for reading fast huge datasets and representing them to :code:`DataFrame` objects.
Moreover data-frames are very well known inside a big-range of disciplines that intersect with data-science, from stasticians to sociologists.

Combining all the information that we had by posing the above questions we reached the conclusion of desinging 6 separate but similar in principle data-structures.
We first separated instantaneous-data-frames with interval-data-frames, as everyone emphasized us the performance gain of making this distinction both in higher and in lower level. Secondly concerning interval-data-frames we made the distinction between continuous and discrete data-frames, as in its case there is different information in relation with intervals (bounds), instants are represented in a different way and operations such as union and difference act in a different way (e.g. in discrete intervals union of :code:`[1, 5]` and :code:`[6, 8]` equals :code:`[1, 8]`, where in continuous this is not the case). Finally a weighted version of all the above data-frames was defined, allowing also the definition of custom functions between weights.

So after following this formulation our objects would look something like:

**TimeSet**

+------------------------------------------+
|      Time-related-columns                |
+==========================================+
|  instants, discrete/continuous intervals |
+------------------------------------------+

**TemporalNodeSet**

+---------+------------------------------------------+
|    u    |      Time-related-columns                |
+=========+==========================================+
| Node-id |  instants, discrete/continuous intervals |
+---------+------------------------------------------+


**TemporalLinkSet**

+---------+---------+------------------------------------------+--------+
|    u    |    v    |           Time-related-columns           |   w*   |
+=========+=========+==========================================+========+
| Node-id | Node-id |  instants, discrete/continuous intervals | weight |
+---------+---------+------------------------------------------+--------+

Time-reated columns are:

- :code:`ts` for instantaneous objects
- :code:`ts, tf` for objects with discrete intervals
- :code:`ts, tf, s, f` for objects with continuous intervals, where :code:`s, f` are flags that are true if the start and the finish is closed or :code:`ts, tf, itype`, where :code:`itype` is for IO and takes values as the :code:`closed` parameter of the `pandas Interval <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Interval.html>`_ ('left', 'right', 'neither', 'both').

Finally the asterisk (*) in the weight on TemporalLinksSet stands for optional.

To understand these common desing princliples, please proceed to the next section, to learn more about our interval-data-frames.
