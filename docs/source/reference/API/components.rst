Components - Units and Collections
===================================

An ``ezmsg`` pipeline is created from a few basic components.
``ezmsg`` provides a framework for you to define your own graphs using its building blocks.
Inherit from its base components to define a pipeline that works for your project.

.. automodule:: ezmsg.core

Most ``ezmsg`` classes intended for use in building pipelines are available in ``ezmsg.core``.
It is convention to ``import ezmsg.core as ez`` and then use this shorthand in your code. e.g.,

.. code-block:: python

   class MyUnit(ez.Unit):
       ...

The two types of nodes in an ezmsg pipeline are ``Unit`` and ``Collection``.

Component
----------
The base class for ``Unit``\ s and ``Collection``\ s.

.. autoclass:: Component

Unit
---------

The nodes of an ezmsg pipeline graph are ``Unit``\ s.

.. autoclass:: Unit
   :show-inheritance:
   :members:
   :inherited-members:


Collection
------------

A ``Collection`` is a special type of ``Component`` that contains other ``Component``\ s (``Unit``\ s and/or other ``Collection``\ s).

.. autoclass:: NetworkDefinition

.. autoclass:: Collection
   :show-inheritance:
   :members:


Component Interaction
---------------------

Two fundamental attributes of a ``Component`` are its ``Settings`` and ``State``, both of which are optional but initialised by the ezmsg backend during ``Unit`` initialisation (if present).

.. autoclass:: Settings

.. autoclass:: State


Stream
------

Facilitates a flow of ``Messages`` into or out of a ``Component``.

.. autoclass:: InputStream

.. autoclass:: OutputStream

Custom Exceptions
-----------------

These are custom exceptions defined in ezmsg.

.. autoclass:: Complete

.. autoclass:: NormalTermination