Collections
===========

In this small section we will examine a small but import part of the library. Found under :code:`stream_graph.collections`, there are four collections under the names of :code:`NodeCollection`, :code:`LinkCollection`, :code:`TimeGenerator`, :code:`TimeCollection`, :code:`TimeSparseCollection`.

Key-Collections
---------------
:code:`NodeCollection` and :code:`LinkCollection` are dictionaries, from whose definition we want to signify that their keys are nodes or links, respectively. Differently with dictionaries, iterating over node collection returns the items of the dictionary, that is a tuple of key and value. Also a :code:`map` method, provided a function can transform all values of the dictionary according to keys and values.

Time-Collections
----------------
Time collections have as their root the :code:`TimeGenerator`. This object represents a :code:`generator` of time-value tuples ascending in time. It can be both *instantaneous* or *discrete*. If time-tuples are instantaneous the generator represents insants of the form :code:`(time-instant, value)`. In case time generators are not instantaneous, there exist two different representations depending on the case, which can both be conceived as representations of step functions. The logic in both is that the time-element of the tuple represents the start of an interval. Whenever a new-element is introduced, we expect a new value. If this time-element is discrete then it correspond to a valid integer. In a different case it corresponds to a tuple containing a timestamp and boolean value describe if the value is contained in this time-stamp or not.
In order to show by example how this interval looks like, let's take the classic heavy-side function.


.. image:: ../_images/heavy-side.svg
   :width: 600

If we want to represent this function in the whole span we observe it, its representation in the discrete time would be: :code:`(-2, 0), (0, 0.5), (1, 1)`, whereas in continuous :code:`((-2, True), 0), ((0, True), 0.5), ((0, False), 1)`.

Moreover the method :code:`tg.merge` which based on another time-generator with common time signature, merges the two into one, as the result of a provided :code:`measure` function, that combines their values. Also as in the case of key-collections their exists a function, that based on the value of a time-value transforms this value to a new one given both the time-element and the value.

The :code:`Time-Collection` object takes an iterator, transforms it to :code:`Time-Generator` and then crystallizes it to a list.
We can take advantage of a sorted list of time-elements, by providing binary-search functions, such as :code:`search_time`, which given a time-element returns the index of a match (or None if we have no-match) and :code:`get_at`, which returns an element or a :code:`not_found` value.

Finally the :code:`TimeSparseCollection` allows to store information, that are the changes of a step function instead of the values. It operates only with objects added as a set and takes a function as input called a :code:`caster`, which casts these sets when iterated. It's input is a tuple of three element, a time-element (it is applied only in non-instantaneous cases) a value that is a set and boolean variable signifying if this value should be added or removed from a main current value holder.

Data-Cube
---------
