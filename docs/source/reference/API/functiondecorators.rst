Unit Function Decorators
******************************************

These function decorators can be added to member functions of an ezmsg ``Unit`` or ``Collection``.

.. autodecorator:: ezmsg.core.subscriber

.. autodecorator:: ezmsg.core.publisher

.. autodecorator:: ezmsg.core.main

.. autodecorator:: ezmsg.core.thread

.. note::
   ``@ez.thread`` is deprecated and will be removed in a future release.
   Prefer explicit background work via ``loop.run_in_executor(...)``.

.. autodecorator:: ezmsg.core.task

.. autodecorator:: ezmsg.core.process

.. autodecorator:: ezmsg.core.timeit
