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

Unit
---------

.. autoclass:: Unit
   :show-inheritance:
   :members:
   :inherited-members:


Collection
------------

.. autoclass:: Collection
   :show-inheritance:
   :members:


Both ``Unit`` and ``Collection`` inherit from the base class ``Component``.

Base Classes
--------------

.. autoclass:: Component

.. autoclass:: NetworkDefinition




Component Interaction
---------------------

.. autoclass:: Settings

.. autoclass:: State


Stream
------

Facilitates a flow of ``Messages`` into or out of a ``Component``.

.. autoclass:: InputStream

.. autoclass:: OutputStream

Custom Exceptions
-----------------

.. autoclass:: Complete

.. autoclass:: NormalTermination