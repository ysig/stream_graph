Current implementations
=======================

In this section we will discuss the initialization proccess of all our current implementations of ABCs, as well as some of their particularities.

Starting with the :code:`NodeSetS`, :code:`ns` this object can be constructed given an iterable of node-identifiers or either a set, which corresponds to its native object. To access the set, a property :code:`ns.nodes` returns a copy of it and :code:`ns.nodes_` the protected attribute of the class.

Moreover the :code:`LinkSetDF` can be defined as an iterable of links (node-pairs) and if parameter :code:`weighted` is :code:`True` it should be given a pair of three elements (node identifiers and a weight), or a pandas DataFrame with at least two columns :code:`'u'`, :code:`'v'` and optionally a third :code:`'w'`, which corresponds to its native object.
To access the dataframe, a property :code:`ns.df` returns a copy of it and :code:`ns.df_` the protected attribute of the class.
As our implementations generally rely on Pandas DataFrames, the node identifiers its better to be restricted to strings or integers, where integers are preferable, as pandas :code:`DataFrame` objects are in this case more efficient.

Next we have our first temporal-object, the :code:`ITimeSetS`, which in order to be initialized expects an iterable of time-instants (that is real numbers), described as a set and a :code:`discrete` argument defining the time-signature. If ommited it assigned to :code:`True` if all the time-instants are integers. Again in this case :code:`ns.times` returns a copy of it the time-set and :code:`ns.times_` the protected attribute.
Moreover we have a collection of similarly defined instantaneous temporal objects :code:`ITemporalNodeSetDF`, :code:`ITemporalLinkSetDF` which can be initialized as :math:`(k*_{1}, .., k*_{2}, t_{s}, w*)`, dependent on how many keys they have and only in the case of the instantaneous temporal-link-set on if the parameter :code:`weighted` is :code:`True`. A dataframe can be also given as input, where the columns :code:`u` and :code:`v` can be provided as keys (for one or for two nodes), :code:`ts` for time-instants, while the weight column if existing should be signified as 'w'.

Finally all non-instantaneous temporal objects :code:`TimeSetDF`, :code:`TemporalNodeSetDF`, :code:`TemporalLinkSetDF`, have a common way of receiving input.
If parameter :code:`discrete` is False, the input is expected to be an iterable of the form :math:`(k*_{1}, .., k*_{n}, t_{s}, t_{f}, w*)`, where the * stands for optional parameters (if there are keys as in the case of temporal-node-set-df or temporal-link-set-df and if there are weights (only in the case of the temporal-link-set if :code:`weighted` is :code:`True`).
Also a dataframe can be given as input, where the columns :code:`u` and :code:`v` can be provided as keys (for one or for two nodes) and the next two should be :code:`ts` and :code:`tf` while the weight column if existing should be signified as 'w'.
In case we have a non-discrete dataframe, the input is expected to be an iterable of the form :math:`(k*_{1}, .., k*_{n}, t_{s}, t_{f}, itype*, w*)` or :math:`(k*_{1}, .., k*_{n}, t_{s}, t_{f}, s*, f*, w*)`, following the same formalism as before with the difference that :code:`itype` (interval-type) signifies the type closed interval bounding ('both', 'open', 'left', 'right') and :code:`s` and :code:`f` are booleans notifying if the start of the interval and the end are closed or not. If :code:`itype` or both :code:`s` and :code:`f` are ommited, an argument :code:`default_closed`, that by default is 'both' in non-weighted cases and 'left' in weighted, signifies the case of the bounding of the interval, if not defined.  
Additionally a data-frame can be provided containing the columns :code:`u` and :code:`v`, provided as keys and the next two should be :code:`ts` and :code:`tf`. Additional columns could be any of :code:`s`, :code:`f` or :code:`itype`. If :code:`s` or :code:`f` is not provided its value is given through :code:`default_closed`. Finally for weighted input :code:`w` can be provided.

Additionally a parameter :code:`disjoint_intervals` signifies if the input contains only intervals that are disjoint (that have no overlaps), as this is important for the objects to function correctly. If :code:`disjoint_intervals` is set to :code:`False`, a function will be executed with the purpose of making them disjoint. Note here, that we make the assumption that links or nodes with same time-signature and same keys are considered duplicate entries and not different nodes. For the instantaneous case, this is given as an argument with the name :code:`no_duplicates`.
In the case of 'weighted' functions a :code:`merge_function` could be defined such that it summarizes same-weights. More-over an algebra for weights can be defined as a dictionary given in a property called :code:`operation_functions`. These functions are those of 'union' or 'u', 'intersection' or 'i', 'difference' or 'd' and 'issuperset' or 's'.
In both cases if the objects are weighted the user should provide weights for all inputs.
All temporal-objects that end with a :code:`DF` have as a native object, one of those defined in the subpackage :code:`stream_graph.base.dataframes`, discussed in the next section.
To end, we should note that as we store the information as a stream, for all methods of dataframe objects (:code:`ending` with :code:`DF`), there is huge computational difference between these two equivalent (in terms of value) calculations:

.. code-block:: python
   :emphasize-lines: 1

   assert KeyCollection({k: obj.attribute_of(u=u, ...) for k in keys}) == obj.attribute_of(u=None, ...)

