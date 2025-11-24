Utility Units and Classes
===========================

Utility :class:`ezmsg Units <ezmsg.core.Unit>` and associated classes for `ezmsg` pipelines. Currently includes the following Units:

- :class:`DebugLog <ezmsg.util.debuglog.DebugLog>`: An ezmsg Unit for logging message debug information. Convenient way to print message contents without modifying other units.
- :class:`TerminateOnTimeout <ezmsg.util.terminate.TerminateOnTimeout>`: An ezmsg Unit that terminates the pipeline after a specified timeout period.
- :class:`TerminateOnTotal <ezmsg.util.terminate.TerminateOnTotal>`: An ezmsg Unit that terminates the pipeline after a specified total number of messages have been processed.
- :class:`MessageGate <ezmsg.util.messagegate.MessageGate>`: An ezmsg Unit that blocks messages based on a condition: open, open after N messages, closed, or closed after N messages.
- :class:`MessageLogger <ezmsg.util.messagelogger.MessageLogger>`: An ezmsg Unit that logs all messages passing through it to a file.
- :class:`MessageQueue <ezmsg.util.messagequeue.MessageQueue>`: An ezmsg Unit that is placed between two other Units to induce backpressure.
- :class:`MessageCollector <ezmsg.util.messagereplay.MessageCollector>`: An ezmsg Unit that collects messages into a local list.
- :class:`MessageReplay <ezmsg.util.messagereplay.MessageReplay>`: An ezmsg Unit that streams messages from :class:`MessageLogger <ezmsg.util.messagelogger.MessageLogger>` created files.

and utility classes for encoding/decoding messages:

- :class:`MessageDecoder <ezmsg.util.messagecodec.MessageDecoder>`: Utility class that decodes ezmsg messages from their JSON representation.
- :class:`MessageEncoder <ezmsg.util.messagecodec.MessageEncoder>`: Utility class that encodes ezmsg messages to their JSON representation.

DebugLog
--------

.. automodule:: ezmsg.util.debuglog
   :show-inheritance:
   :members:


Terminate
---------

.. automodule:: ezmsg.util.terminate
   :show-inheritance:
   :members:

MessageReplay
-------------

.. automodule:: ezmsg.util.messagereplay
   :show-inheritance:
   :members:


MessageGate
-----------

.. automodule:: ezmsg.util.messagegate
   :show-inheritance:
   :members:


MessageLogger
-------------

.. automodule:: ezmsg.util.messagelogger
   :show-inheritance:
   :members:


MessageQueue
------------

.. automodule:: ezmsg.util.messagequeue
   :show-inheritance:
   :members:

MessageCodec
------------

.. automodule:: ezmsg.util.messagecodec
   :show-inheritance:
   :members:
