The :code:`stream_graph.base.dataframes` subpackage
===================================================


At this section we will try to describe how we designed our core data-structures.
We defined 6 different variation of the classic :code:`pandas.DataFrame`, namely the:

- InstantaneousDF and InstantaneousWDF 
- CIntervalDF and CIntervalWDF
- DIntervalDF and DIntervalWDF

where 'DF' stands for DataFrame, 'W' for weighted, 'C' for continuous and 'D' for discrete.

In order to initialize a dataframe, the procedure is similar to pandas, with the difference that some time and weight columns names should be set to certain values in order to be recognizable: :code:`ts` for instantaneous, :code:`ts`, :code:`tf` for discrete intervals and :code:`ts`, :code:`tf`, :code:`s`, :code:`f` for continuous (where the **boolean** columns :code:`s`, :code:`f`, respectively signify if the start or the finish bound is closed). Moreover the weighted interval-data-frames should expect another column :code:`w` sigifying the weight of each entry.

Upon initialization the user should signify, if the data-frame (surely) has no overlapping elements in the time domain. This is possible through the boolean parameter :code:`no_duplicates` in the case of instantaneous-data-frames and :code:`disjoint_intervals` in the case of interval-data-frames. If the time elements overlap, a proccess called :code:`merge` (respectively for each case) will take place upon initialization, as in order for the other methods to function correcty **the time-elements of common keys shouldn't overlap**. A default :code:`merge_function` can be defined for each object, designating how to merge a data-frame's weights on a variety of occassions (:code:`drop`, :code:`__get_item__`, ...)


The dataframe operations
========================
In this basis a list of operations are defined common to all data-structures.
In all of them, there is no change of dimensionality in the produced data-frame.
Those who have the same dimensionality allow the result to be updated on self object, through an :code:`inplace` boolean parameter.

:code:`merge`
-------------
In its most general form merge can be defined as the operation that summarizes overlapping time elements on common keys.
On non-weighted elements multiple duplicate time-instants, will be reduced to a single instant.

For a continuous interval the rule is that two intervals :math:`(t_{s}^{a}, t_{f}^{a}, s^{a}, f^{a})` and :math:`(t_{s}^{b}, t_{f}^{b}, s^{b}, f^{b})`with :math:`t_{s}^{a} <= t_{f}^{b}` are replaced by a common interval :math:`(t_{s}^{a}, t_{f}^{b}, s^{a}, f^{b})` iff :math:`t_{f}^{a} > t_{s}^{b}` \lor (:math:`t_{f}^{a} = t_{s}^{b}\land (f^{a}\;or\;s^{b}))`.

For a discrete interval the rule is that for two intervals :math:`(t_{s}^{a}, t_{f}^{a})`, :math:`(t_{s}^{b}, t_{f}^{b})` with :math:`t_{s}^{a} \leq t_{f}^{b}` are replaced by a common interval :math:`(t_{s}^{a}, t_{f}^{b})` iff :math:`t_{f}^{a} \geq t_{s}^{b} - 1`.

For weighted cases a :code:`merge_function` to a list of :code:`args`, concerning all the weights assigned on each overlaping time-element, the one which is defined upon initialization. Intervals or instants are then merged if they follow the above laws for each case, having the same weight.


:code:`union`
-------------
Similar to :code:`merge` but not identical in all cases, :code:`union` in its straight form takes two data-frames with common key-signature (for example such that both have the same keys with the same names :code:`u, v`) and for each common key it takes their union which has the same unordered rules in all cases, with :code:`merge`.

If weights exist, then we keep the weights of the first or of the second data-frame on non-overlapping time-elements respectively. On overlapping elements the weight is produced by a union-function calculated between the two weights (as the starting two dataframes are merged, there exist at-most two weights on overlap). Intervals then, that have the same-weight are then merged if they follow the above laws of merging intervals.

To demonstrate a difference between a weighted case and not weighted one, please observe the union of the following example given that our union function is the sum :code:`+`.

Given :math:`df_{a}`:

===  ======  ====  ====  ===  ===  ===
u    v         ts    tf  s    f      w
===  ======  ====  ====  ===  ===  ===
bee  flower     1     3  T    F      2
bee  flower     3     5  T    T      1
===  ======  ====  ====  ===  ===  ===

and :math:`df_{b}`:

===  ======  ====  ====  ===  ===  ===
u    v         ts    tf  s    f      w
===  ======  ====  ====  ===  ===  ===
bee  flower     1     3  T    F      1
bee  flower     3     5  T    T      2
===  ======  ====  ====  ===  ===  ===

their union :math:`df_{ab}`, will be:

===  ======  ====  ====  ===  ===  ===
u    v         ts    tf  s    f      w
===  ======  ====  ====  ===  ===  ===
bee  flower     1     5  T    T      3
===  ======  ====  ====  ===  ===  ===

:code:`intersection`
--------------------
Intersection in its most general form, can be defined as the operation that keeps only the overlapping time elements on common keys, between two data-frames.
On non-weighted elements only duplicate key-time entries, will be in the result of this method.

For a continuous interval the rule is that two intervals :math:`(t_{s}^{a}, t_{f}^{a}, s^{a}, f^{a})`, :math:`(t_{s}^{b}, t_{f}^{b}, s^{b}, f^{b})` with :math:`t_{s}^{a} \neq t_{f}^{b}`, produce a valid interval :math:`t_{s}^{i},\; t_{f}^{j},\; s_{i}\; if\; t_{s}^{a}\; \neq\; t_{s}^{b}\;else\;min(s^{a},\;s^{b}),\;f_{j}\;if\;t_{f}^{a} \neq t_{f}^{b}\;else\; min(f_{1}, f_{2})))`, where :math:`argmax_{i}(t_{s}^{i}),\;argmax_{j}(t_{s}^{j})` **iff** :math:`t_{f}^{a} > t_{s}^{b}\lor (t_{f}^{a} = t_{s}^{b}\land f^{a}\land s^{b})`.

For a discrete interval the rule is that for two intervals :math:`(t_{s}^{a}, t_{f}^{a})` and :math:`(t_{s}^{b}, t_{f}^{b})`with :math:`t_{s}^{a} \leq t_{f}^{b}` are replaced by a common interval :math:`(t^{a}_{s}, t^{b}_{f})` iff :math:`t_{f}^{a} \geq t_{s}^{b} - 1`.

For weighted cases a intersection-function is applied between the two weights occuring on each overlaping time-element. If this function returns a :code:`None` this time-element is ignored.


:code:`difference`
------------------
Difference is in its most general form, the binary operation that keeps the parts of its reference data-frame that do not overlap with the time elements of the second.
As so, on non-weighted elements only time-instants that appear only in the first dataframe, will be in the result of this method.

For continuous intervals the rule is that two intervals :math:`(t_{s}^{a},\; t_{f}^{a},\;  s^{a},\; f^{a})` and :math:`(t_{s}^{b},\; t_{f}^{b},\; s^{b},\; f^{b})` with :math:`t_{s}^{a} \leq t_{s}^{b}` **or** :math:`t_{f}^{a} \leq t_{f}^{b}`, produce a valid interval :math:`(t_{s}^{a},\; t_{s}^{b},\; s^{a},\; \lnot s^{b})` **or** a valid interval :math:`(t_{f}^{a}, t_{f}^{b}, \lnot f^{a}, f^{b})` if :math:`t_{s}^{a} < t_{s}^{b}\lor (t_{s}^{a} = t_{s}^{b}\land s^{a} > s^{b})` **or** if :math:`t_{f}^{a} > t_{f}^{b}\lor (t_{f}^{a} = t_{f}^{b}\land f^{a} > f^{b})`.  

In the discrete case the rule is that two intervals :math:`(t_{s}^{a},\; t_{f}^{a})` :math:`(t_{s}^{b},\; t_{f}^{b})` with :math:`t_{s}^{a} \leq t_{s}^{b}` **or** :math:`t_{f}^{a} \leq t_{f}^{b}`, produce a valid interval :math:`(t_{s}^{a}, t_{s}^{b} - 1)` **or** a valid interval :math:`(t_{f}^{a} + 1, t_{f}^{b})` if :math:`t_{s}^{a} \leq t_{s}^{b} - 1` **or** if :math:`t_{f}^{a} \geq t_{f}^{b} + 1`.

For weighted cases, the weights of the intervals of the first dataframe are conserved in non-overlapping time-elements. A difference-function is afterwards applied between the two weights occuring on each overlaping time-element. If this function returns a :code:`None` this time-element is ignored.

:code:`issuperset`
------------------
Issuperset in its most general form, is the boolean function that checks if all the elements of the second data-frame are all contained in the first.
As so all time-instant-key entries of the second data-frame, should be contained in the first.

In the continuous case for each interval of the second data-frame :math:`(t_{s}^{b},\; t_{f}^{b},\; s^{b},\; f^{b})` there should exist an interval :math:`(t_{s}^{a},\; t_{f}^{a},\;  s^{a},\; f^{a})` in the first such that :math:`(t_{s}^{a} < t_{s}^{b}\lor (t_{s}^{a} = t_{s}^{b}\land s^{a} \not < s^{b}))\land (t_{f}^{a} > t_{f}^{b}\lor (t_{f}^{a} = t_{f}^{b}\land f^{a} \not < f^{b}))`.

In the discrete case for each interval of the second data-frame :math:`(t_{s}^{b},\; t_{f}^{b})` there should exist an interval :math:`(t_{s}^{a},\; t_{f}^{a})` in the first such that :math:`(t_{s}^{a} \leq t_{s}^{b}) and\;(t_{f}^{a} \geq t_{f}^{b})`.

For weighted cases, after satisfying the above conditions, a function is calculated between the weights of the container and the contained interval, which if it returns False for one occasion, the whole function returns true.


:code:`nonempty_intersection`
-----------------------------
This function checks if there the result of the intersection between two dataframes is not empty, following the laws of a valid intersection as defined above.  
For weighted cases a function is defined such that a common-time element is accepted iff this function between the two associated weights returns True.

* Note: All the above operations (except merge) can be also called by having as a second element a dataframe without keys. When done so it is as if we apply this operation between all the time-elements associated with each key of the first data-frame and the time-elements of the second data-frame. This is possible upon the call of the method, by setting the argument :code:`by_key` to false.


:code:`cartesian_intersection`
------------------------------
This function in a naive form takes two arguments that correspond to a temporal-link-set :math:`L = U_{a} \times U_{b} \times T^{L}` and a temporal-node-set :math:`N = V \times T^{N}`. First it calculates the cartesian product :math:`CP = V_{a} \times V_{b} \times T^{CP}` on the vertices of the TemporalNodeSet, such that that for each two nodes :math:`u, v \in V \times V`, :math:`T_{u, v}^{CP} = T_{u}^{N} \cap T_{v} ^{N}`. The *cartesian intersection between* :math:`L` and :math:`N`, :math:`CI = (U_{a} \cap V) \times (U_{b} \cap V) \times T^{CI}`, is such that if two nodes :math:`u, v \in (U_{a} \cap V) \times (U_{b} \cap V)`, :math:`T_{u, v}^{CP} = T_{u, v}^{L} \cap T_{u, v}^{CP} = T_{u, v}^{L} \cap T_{u}^{N} \cap T_{v} ^{N}`. This function is helpful for extracting the temporal-link-set defined inside a given temporal-node-set (backend of the ABC implementation of :code:`tls.induced_substream`).

The rules of intersection of time-elements are the same with the intersection function.
In the weighted case a function taking three arguments is applied between a weight entry of the TemporalLinkSet and the weight entry of each of the other two corresponding nodes of the TemporalNodeSet.

:code:`map_intersection`
------------------------------
This function in a naive form, takes two arguments that correspond to a temporal-link-set :math:`L = U_{a} \times U_{b} \times T^{L}` and a temporal-node-set :math:`N = V \times T^{N}` and returns a temporal-node-set :math:`TN = V^{TN} \times T^{TN}`, which corresponds to the *temporal neighborhood* of :math:`N` in :math:`L`. If we define with :math:`N^{L}(u) = \{(v, t) : (u, v, t) \in L\}` and :math:`N(u) = \{t: (u, t) \in V\}`
then the temporal-neighborhood of :math:`N` in :math:`L`, is :math:`TN = \cup_{u} \{(v, t):t \in N(u) \land (v, t) \in N^{L}(u) \}`.
Namely is the union of the intersection with :code:`by_key=False` of each time-set related to each node on the temporal-node-set with the associated it's temporal-node-set extracted from this node in the temporal-link-set: :math:`\cup_{u} N^{L}(u) \cap_{\texttt{by_key=False}} N(u)`.
This operation has not until now been generalized to weighted cases, beacuase there was no need as we haven't defined a weighted temporal-node-set.


:code:`interval_intersection_size`
----------------------------------
Based on the rules previously defined, this function calculates the size of the intersection between two-dataframes. As mentioned before, in the continuous case the size of a continuous interval :math:`(t_{s}^{a},\; t_{f}^{a},\;  s^{a},\; f^{a})` is equal to :math:`t_{f}^{a} - t_{s}^{a}`. In the discrete case the size of an interval :math:`(t_{s}^{a},\; t_{f}^{a})` is equal to :math:`t_{f}^{a} - t_{s}^{a} + 1`. In the case of instants as size exists only in the discrete case and as so function is useful for calculating in general the number of common key, time-element entries.

In the weigted case, a function is applied between two collections of weighted elements from the first data-frame and of weighted elements for the second data-frame, which coexist at the same combination of key and time elements.

Algorithm implementation
------------------------

Without going into much detail, we will try to describe our basic algorithmic design, which is common in all of the above algorithms.
We would first like to concentrate to algorithms for intervals.
All interval elements can be deconstructed in what we call *events*. Instead of having the interval :code:`[2, 3]`, what we can have is two events :code:`(2, True), (3, False)` signifying that an interval starts and an interval finishes. Moreover a signifier, which is refered inside the code as *reference* (boolean as we have two) can optionally describe to which category an interval belongs.
Later each algorithm can be constructed, such that based in a certain ordering on equivalent occurencies of ascending time (and following the assumption, that each interval is valid and in some cases that the orignal intervals in each data-frame are merged) a function is executed which considering some previous information stored in what we call *a cache* for each active key (if we have keys) updates this information or outputs a part of the result (value or interval).
All these algorithms can be found in :code:`stream_graph.base.dataframes.algorithms`, while there exists a big amount of orderings which are designed in relation to each *update* function.
These orderings take into account a certain form of events (defined in :code:`stream_graph.base.dataframes.algorithms.utils.{no_bounds, bounds}`), where all are base on ascending time, while having a different ordering deppending on the type of the bound and the reference if they exist and the start. As so an ordering starting with :code:`rb_order_0n3_2`, means that this is an order of tuples with reference flags and bound (closed) flag, where the key-function (as in python :code:`sorted`) returns :code:`(k[1], k[0]!=k[3], k[2])` on a given key tuple. This tuples are generally of the form :code:`(r, t, c, s, ..)`, where if :code:`b` is ommited from the definition, :code:`c` is considered to be missing and if :code:`r` is ommited, :code:`c` is also.
To see the effect of the orderings, you can run the file :code:`stream_graph.base.dataframes.algorithms.utils.orderings` as is.
