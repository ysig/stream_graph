Collections
===========

In this small section we will examine a rather small, but important part of the library. Found under :code:`stream_graph.collections`, there are 5 collections under the names of :code:`NodeCollection`, :code:`LinkCollection`, :code:`TimeGenerator`, :code:`TimeCollection`, :code:`TimeSparseCollection`.

Key-Collections
---------------
:code:`NodeCollection` and :code:`LinkCollection` are dictionaries, from whose definition we want to signify that their keys are nodes or links, respectively. Differently with dictionaries, iterating over a node-collection returns the items of the dictionary, that is a tuple of key and value. Also a :code:`map` method, provided a function can transform all values of the dictionary according to their keys and values.

Time-Collections
----------------
Time collections have as their root the :code:`TimeGenerator`. This object represents a :code:`generator` of time-value tuples ascending in time. It can be both *instantaneous* or *discrete*. If time-tuples are instantaneous the generator represents insants of the form :code:`(time-instant, value)`. In case time generators are not instantaneous, there exist two different representations depending on the case, which can both be conceived as representations of step functions. The logic in both, is that the time-element of the tuple represents the start of an interval. Whenever a new-element is introduced, we expect a new value. If this time-element is discrete then it correspond to a valid integer. In the opposite case, it corresponds to a tuple containing a timestamp and a boolean value describes, if the given value, is contained in this time-stamp or not.
In order to show by example how this interval looks like, let's take the classic heavy-side function.


.. image:: ./heavy-side.svg
   :width: 600

If we want to represent this function in the whole span that it is plotted, its representation in the discrete time would be: :code:`(-2, 0), (0, 0.5), (1, 1)`, whereas in continuous :code:`((-2, True), 0), ((0, True), 0.5), ((0, False), 1)`.

Moreover the :code:`tg.merge` method, merges another time-generator with common time signature with the original to a new one, provided a :code:`measure` function, that combines their values. Also as in the case of key-collections their exists a :code:`tg.map` method, that tranforms transforms the value of a time-value pair, based on a given function.

The :code:`Time-Collection` object takes an iterator, which it transforms to a :code:`Time-Generator` and then stores it as a list.
We can take advantage of a sorted list of time-elements, by providing binary-search functions, such as :code:`search_time`, which given a time-element returns the index of a match (or :code:`None` if we have no-match) and :code:`get_at`, which returns an element or a :code:`not_found` value.

Finally the :code:`TimeSparseCollection` allows to store information, that are the changes of a step function instead of its values. It operates only with set-objects and takes a function as input called a :code:`caster`, which casts the accumulated set at each time-element, when iterated. It's input is a tuple of three elements, a time-element (it is applied only in non-instantaneous cases) a value that is a set and boolean variable signifying if this value should be added or removed from a main current set value *holder*.

Data-Cube
---------
A `data-cube <https://en.wikipedia.org/wiki/Data_cube>`_ is a certain abstraction of a objects with keys, as well as instantaneous-time-objects.
If initialized the data-cube is basically an n-dimensional array of interactions, that have a certain value.
Based on this idea Audrey Wilmet and Robin Lamarche Perrin, formalized a set of measures in this data-structure translatable from the instantaneous-weighted-temporal-linkset in their paper for `Multidimensional Outlier Detection in Twitter <https://arxiv.org/abs/1906.02541>`_, which has (for now) a minimal but general implementation in this package.
