`ezmsg` Entry Point
========================

An ``ezmsg`` pipeline is created from a few basic components.
``ezmsg`` provides a framework for you to define your own graphs using its building blocks.
Inherit from its base components to define a pipeline that works for your project.

.. automodule:: ezmsg.core

Most ``ezmsg`` classes intended for use in building pipelines are available in ``ezmsg.core``.
It is convention to ``import ezmsg.core as ez`` and then use this shorthand in your code. e.g.,



Entry Point
-----------

.. automethod:: ezmsg.core.run
