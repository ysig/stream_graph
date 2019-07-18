Current implementations
------------------------

As we store the information as a stream, there is huge computational difference between these two equivalent calculations:

.. code-block:: python
   :emphasize-lines: 1

   assert {u: obj.attribute_of(u=u, ...) for u in keys} == obj.attribute_of(u=None, ...)
