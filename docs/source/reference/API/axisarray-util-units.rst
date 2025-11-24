AxisArray Utility Methods and Units
=================================================

Utility classes which implement functionality related to messages. These include a method for instantiating new instances of a dataclass given an existing instance:

- :class:`replace <ezmsg.util.messages.axisarray.replace>` - Function for creating a new dataclass instance by replacing attributes of an existing instance

and several :class:`ezmsg Unit <ezmsg.core.Unit>` classes which operate on :class:`AxisArray <ezmsg.util.messages.axisarray.AxisArray>` messages:

- :class:`ArrayChunker <ezmsg.util.messages.chunker.ArrayChunker>` - ezmsg Unit to chunk AxisArray messages along specified axes
- :class:`SetKey <ezmsg.util.messages.key.SetKey>` - ezmsg Unit for setting the key of incoming AxisArray messages
- :class:`FilterOnKey <ezmsg.util.messages.key.FilterOnKey>` - ezmsg Unit for filtering an AxisArray based on its key.
- :class:`ModifyAxis <ezmsg.util.messages.modify.ModifyAxis>` - ezmsg Unit for modifying axis names and dimensions of AxisArray messages.

replace
------------

.. autofunction:: ezmsg.util.messages.axisarray.replace


Array Chunking
---------------

.. automodule:: ezmsg.util.messages.chunker
   :show-inheritance:
   :members:


Modifying AxisArray "key"
---------------------------

.. automodule:: ezmsg.util.messages.key
   :show-inheritance:
   :members:


Modifying AxisArray "axes"
------------------------------

.. automodule:: ezmsg.util.messages.modify
   :show-inheritance:
   :members:
