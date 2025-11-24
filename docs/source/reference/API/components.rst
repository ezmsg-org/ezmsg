Components - Units and Collections
===================================

An ezmsg pipeline is created from a few basic components. ezmsg provides a framework for you to define your own graphs using its building blocks. The nodes of the graph that defines a pipeline all inherit from the base class :class:`Component <ezmsg.core.Component>`. The two types of Component are :class:`Unit <ezmsg.core.Unit>` and :class:`Collection <ezmsg.core.Collection>`.

.. note:: It is convention to ``import ezmsg.core as ez`` and then use this shorthand in your code. e.g.,

   .. code-block:: python

      class MyUnit(ez.Unit):
         ...


Component
----------

.. autoclass:: ezmsg.core.Component

Unit
---------

The basic nodes of an ezmsg pipeline graph are :class:`Units <ezmsg.core.Unit>`.

.. autoclass:: ezmsg.core.Unit
   :show-inheritance:
   :members:
   :inherited-members:


Collection
------------

A :class:`Collection <ezmsg.core.Collection>` is a special type of :class:`Component <ezmsg.core.Component>` that contains other :class:`Components <ezmsg.core.Component>` (:class:`Units <ezmsg.core.Unit>` and/or other :class:`Collections <ezmsg.core.Collection>`).

.. autoclass:: ezmsg.core.NetworkDefinition

.. autoclass:: ezmsg.core.Collection
   :show-inheritance:
   :members:


Component Interaction
---------------------

Two fundamental attributes of a :class:`Component <ezmsg.core.Component>` are its :class:`Settings <ezmsg.core.Settings>` and :class:`State <ezmsg.core.State>`, both of which are optional but initialised by the ezmsg backend during :class:`Unit <ezmsg.core.Unit>` initialisation (if present).

.. autoclass:: ezmsg.core.Settings

.. autoclass:: ezmsg.core.State


Stream
------

Facilitates a flow of Messages into or out of a :class:`Component <ezmsg.core.Component>`.

.. autoclass:: ezmsg.core.InputStream

.. autoclass:: ezmsg.core.OutputStream

Custom Exceptions
-----------------

These are custom exceptions defined in ezmsg.

.. autoclass:: ezmsg.core.Complete

.. autoclass:: ezmsg.core.NormalTermination