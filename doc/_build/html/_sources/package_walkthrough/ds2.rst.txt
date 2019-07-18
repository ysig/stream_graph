The :code:`stream_graph.base.dataframes` subpackage
===================================================


At this section we will try to describe how we designed our core data-structures.
We defined 6 different variation of the dataframe, namely the:

- InstanraneousDF and InstantaneousWDF 
- CIntervalDF and CIntervalWDF
- DIntervalDF and DIntervalWDF

where 'DF' stands for DataFrame, 'W' for weighted, 'C' for continuous and 'D' for discrete.

In order to initialize the dataframe, the procedure is similar to pandas, with the difference that some time and weight columns names should be set to certain values in order to be recognizable: :code:`ts` for instantaneous, :code:`ts`, :code:`tf` for discrete intervals and :code:`ts`, :code:`tf`, :code:`s`, :code:`f` for continuous (where the **boolean** columns :code:`s`, :code:`f`, respectively signify if the start or the finish bound is closed). Moreover the weighted interval-data-frames should expect another column :code:`w` sigifying the weight of each entry.

Upon initialization the user should signify, if the data-frame (surely) has no overlapping elements in the time domain. This is possible through the boolean parameter parameter :code:`no_duplicates` in the case of instantaneous-data-frames and :code:`disjoint_intervals` in the case of interval-data-frames. If the time elements overlap a proccess called :code:`merge` (respectively for each case) will take place upon initialization, as in order for the other methods to function correcty **the time-elements of common keys shouldn't overlap**.


The dataframe operations
========================
In this basis a list of operations are defined common to all data-structures.
In all of them there is no change of the dimensionality of the data-frame.
Those who have the same dimensionality allow the result to be updated on self object, through an :code:`inplace` boolean parameter.

:code:`merge`
-------------
In its most general form merge can be defined as the operation that summarizes overlapping time elements on common keys.
On non-weighted elements multiple duplicate time-instants, will be reduced to a single instant.

For a continuous interval the rule is that two intervals :math:`(t_{s}^{a}, t_{f}^{a}, s^{a}, f^{a})` and :math:`(t_{s}^{b}, t_{f}^{b}, s^{b}, f^{b})`with :math:`t_{s}^{a} <= t_{f}^{b}` are replaced by a common interval :math:`(t_{s}^{a}, t_{f}^{b}, s^{a}, f^{b})` iff :math:`t_{f}^{a} > t_{s}^{b}` \lor (:math:`t_{f}^{a} = t_{s}^{b}\land (f^{a}\;or\;s^{b}))`.

For a discrete interval the rule is that for two intervals :math:`(t_{s}^{a}, t_{f}^{a})`, :math:`(t_{s}^{b}, t_{f}^{b})` with :math:`t_{s}^{a} \leq t_{f}^{b}` are replaced by a common interval :math:`(t_{s}^{a}, t_{f}^{b})` iff :math:`t_{f}^{a} \geq t_{s}^{b} - 1`.
For weighted cases a merge-function is applied between all the weights assigned on each overlaping time-element. Intervals are then merged if they follow the above laws for each case, having the same weight.


:code:`union`
-------------
Similar to :code:`merge` but not identical in all cases, :code:`union` in its straight form takes two data-frames with common key-signature (for example such that both have the same keys with the same names :code:`u, v`) and for each common key it takes their union which has the same unordered rules in all cases, with :code:`merge`.

If weights exist the then we keep the weights of the first or of the second data-frame on non-overlapping time-elements respectively. On overlapping elements the weight is produced by union-function calculate between the two weights (as the two original dataframes are merged, there exist at-most two weights on overlap). Intervals then, that have the same-weight are then merged if they follow the above laws of merging interval.

To demonstrate a difference between a weighted case and not weighted one, please observe the union of the following example given that our union function is the sum :code:`+`.

Given :math:`df_{a}`:

===  ======  ====  ====  ===  ===  ===
u    v         ts    tf  s    f      w
===  ======  ====  ====  ===  ===  ===
bee  flower     1     3  T    F      2
bee  flower     3     5  T    T      1
===  ======  ====  ====  ===  ===  ===

and :math:`df_b`:

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
Intersection is in its most general form can be defined as the operation that keep the overlaps between time elements on common keys.
On non-weighted elements only duplicate time-instants, will be in the result of this method.

For a continuous interval the rule is that two intervals :math:`(t_{s}^{a}, t_{f}^{a}, s^{a}, f^{a})`, :math:`(t_{s}^{b}, t_{f}^{b}, s^{b}, f^{b})` with :math:`t_{s}^{a} \neq t_{f}^{b}`, produce a valid interval :math:`t_{s}^{i},\; t_{f}^{j},\; s_{i}\; if\; t_{s}^{a}\; \neq\; t_{s}^{b}\;else\;min(s^{a},\;s^{b}),\;f_{j}\;if\;t_{f}^{a} \neq t_{f}^{b}\;else\; min(f_{1}, f_{2})))`, where :math:`argmax_{i}(t_{s}^{i}),\;argmax_{j}(t_{s}^{j})` **iff** :math:`t_{f}^{a} > t_{s}^{b}\lor (t_{f}^{a} = t_{s}^{b}\land f^{a}\land s^{b})`.

For a discrete interval the rule is that for two intervals :math:`(t_{s}^{a}, t_{f}^{a})` and :math:`(t_{s}^{b}, t_{f}^{b})`with :math:`t_{s}^{a} \leq t_{f}^{b}` are replaced by a common interval :math:`(t^{a}_{s}, t^{b}_{f})` iff :math:`t_{f}^{a} \geq t_{s}^{b} - 1`.

For weighted cases a intersection-function is applied between the two weights occuring on each overlaping time-element. If this function returns a :code:`None` this time-element is ignored.


:code:`difference`
------------------
Difference is in its most general form, the binary operation that keeps the parts of the left data-frame that do not overlaps with the time elements of the second.
As so, on non-weighted elements only time-instants that only appear in the first dataframe will be in the result of this method.

For a continuous interval the rule is that two intervals :math:`(t_{s}^{a},\; t_{f}^{a},\;  s^{a},\; f^{a})` and :math:`(t_{s}^{b},\; t_{f}^{b},\; s^{b},\; f^{b})` with :math:`t_{s}^{a} \leq t_{s}^{b}` **or** :math:`t_{f}^{a} \leq t_{f}^{b}`, produce a valid interval :math:`(t_{s}^{a},\; t_{s}^{b},\; s^{a},\; \lnot s^{b})` **or** a valid interval :math:`(t_{f}^{a}, t_{f}^{b}, \lnot f^{a}, f^{b})` if :math:`t_{s}^{a} < t_{s}^{b}\lor (t_{s}^{a} = t_{s}^{b}\land s^{a} > s^{b})` **or** if :math:`t_{f}^{a} > t_{f}^{b}\lor (t_{f}^{a} = t_{f}^{b}\land f^{a} > f^{b})`.  

In the discrete case the rule is that two intervals :math:`(t_{s}^{a},\; t_{f}^{a})` :math:`(t_{s}^{b},\; t_{f}^{b})` with :math:`t_{s}^{a} \leq t_{s}^{b}` **or** :math:`t_{f}^{a} \leq t_{f}^{b}`, produce a valid interval :math:`(t_{s}^{a}, t_{s}^{b} - 1)` **or** a valid interval :math:`(t_{f}^{a} + 1, t_{f}^{b})` if :math:`t_{s}^{a} \leq t_{s}^{b} - 1` **or** if :math:`t_{f}^{a} \geq t_{f}^{b} + 1`.

For weighted cases, the weights of the intervals of the first dataframe are conserved in non-overlapping time-elements. A difference-function is afterwards applied between the two weights occuring on each overlaping time-element. If this function returns a :code:`None` this time-element is ignored.

:code:`issuperset`
------------------
Issuperset in its most general form, is the boolean function that checks if all the elements of the second data-frame are all contained in the first another.
As so all time-instants of the second data-frame, should be contained in the first.

In the continuous case for each interval of the second data-frame :math:`(t_{s}^{b},\; t_{f}^{b},\; s^{b},\; f^{b})` there should exist an interval :math:`(t_{s}^{a},\; t_{f}^{a},\;  s^{a},\; f^{a})` in the first such that :math:`(t_{s}^{a} < t_{s}^{b}\lor (t_{s}^{a} = t_{s}^{b}\land s^{a} \not < s^{b}))\land (t_{f}^{a} > t_{f}^{b}\lor (t_{f}^{a} = t_{f}^{b}\land f^{a} \not < f^{b}))`.

In the discrete case for each interval of the second data-frame :math:`(t_{s}^{b},\; t_{f}^{b})` there should exist an interval :math:`(t_{s}^{a},\; t_{f}^{a})` in the first such that :math:`(t_{s}^{a} \leq t_{s}^{b}) and\;(t_{f}^{a} \geq t_{f}^{b})`.

For weighted cases, after satisfying the above conditions, a function is calculated between the weights of the container and the contained interval, which if it returns False for one occasion, the whole returns true.


:code:`nonempty_intersection`
-----------------------------
This function checks if there the result of the intersection between two dataframes is not empty, following the laws of a valid intersection as defined above.  
For weighted cases a function is defined such that an common-time element is accepted iff this function between the two associated weights returns True.


* Note: All the above operations (except merge) can be also called by having as a second element a dataframe without keys. When done so it is as if we apply this operation between all the time-elements associated with each key of the first data-frame and the time-elements of the second data-frame. This is possible by setting upon call of the method the argument :code:`by_key` to false.


:code:`cartesian_intersection`
------------------------------
This function in a naive form takes two arguments that correspond to a temporal-link-set :math:`L = U_{a} \times U_{b} \times T^{L}` and a TemporalNodeSet :math:`N = V \times T^{N}`. First it calculates the cartesian product :math:`CP = V_{a} \times V_{b} \times T^{CP}` on the vertices of the TemporalNodeSet, such that that for each two nodes :math:`u, v \in V \times V`, :math:`T_{u, v}^{CP} = T_{u}^{N} \cap T_{v} ^{N}`. The *cartesian intersection between* :math:`L` and :math:`N` is such :math:`CI = (U_{a} \cap V) \times (U_{b} \cap V) \times T^{CI}`, such that if two nodes :math:`u, v \in (U_{a} \cap V) \times (U_{b} \cap V)`, :math:`T_{u, v}^{CP} = T_{u, v}^{L} \cap T_{u, v}^{CP} = T_{u, v}^{L} \cap T_{u}^{N} \cap T_{v} ^{N}`. This function is helpful for extracting the temporal-link-set defined inside a given temporal-node-set.

The rules of intersection of time-elements are the same with the intersection function.
In the weighted case a function taking three arguments is applied between a weight entry of the TemporalLinkSet and the weight entry of each of the other two corresponding nodes of the TemporalNodeSet.

:code:`map_intersection`
------------------------------
This function in a naive form, takes two arguments that correspond to a temporal-link-set :math:`L = U_{a} \times U_{b} \times T^{L}` and a temporal-node-set :math:`N = V \times T^{N}` and returns a temporal-node-set :math:`TN = V^{TN} \times T^{TN}`, which corresponds to the *temporal neighborhood* of :math:`N` in :math:`L`. If we define with :math:`N^{L}(u) = \{(v, t) : (u, v, t) \in L\}` and :math:`N(u) = \{t: (u, t) \in V\}`
then the temporal-neighborhood of :math:`N` in :math:`L` and :math:`TN = \cup_{u} \{(v, t):t \in N(u) \land (v, t) \in N^{L}(u) \}`.
By words it is the union of the non-by-key intersection of each time-set related to each node on the temporal-nodeset with the associated temporal-nodeset extracted from this node in the temporal-link-set: :math:`\cup_{u} N^{L}(u) \cap_{\texttt{by_key=False}} N(u)`.
This operation has not until now been generalized to weighted cases, as we haven't defined a weighted temporal-node-set.


:code:`interval_intersection_size`
----------------------------------
Based on the rules previously defined, this function calculates the size of the intersection between two-dataframes. As mentioned before, in the continuous case the size of a continuous interval :math:`(t_{s}^{a},\; t_{f}^{a},\;  s^{a},\; f^{a})` is equal to :math:`t_{f}^{a} - t_{s}^{a}`. In the discrete case the size of an interval :math:`(t_{s}^{a},\; t_{f}^{a})` is equal to :math:`t_{f}^{a} - t_{s}^{a} + 1`. In the case of instants as size exists only in the discrete case, this function counts in general the number of common time-elements.

In the weigted case, a function is applied between two collections of weighted elements from the first data-frame and of weighted elements of the second data-frame, which coexist at the same time elements.

Algorithm implementation
------------------------

Without going into much detail, we will try to describe our basic algorithmic design, which is common in all of the above algorithms.
We would first like to emphasize to algorithms, that concern intervals.
All interval elements can be deconstructed in what we call *events*. Instead of having the interval [2, 3], what we can have is two events (2, True), (3, False) signifying that an interval starts and an interval finishes. Moreover a signifier, which is refered inside the code as *reference* (boolean as we have two) can optionally describe to which of category an interval belongs.
Later each algorithm can be constructed, such that based in a certain ordering on equivalent occurencies of ascending time (and following the assumption that each interval is valid and in some cases that the orignal intervals in each data-frame are merged) a function is executed which considering some previous information stored in what we call *a cache* stored for each active key (if we have keys) updates this information or outputs a part of the result (value or interval).
All this algorithms can be found in :code:`stream_graph.base.dataframes.algorithms`, while there exists a big amount of orderings which change concerning its *update* function.
These orderings take into account a certain form of events (defined in :code:`stream_graph.base.dataframes.algorithms.utils.{no_bounds, bounds}`) and are all base on ascending time, while having a different ordering deppending on the type of the bound and the reference if they exist and the start.