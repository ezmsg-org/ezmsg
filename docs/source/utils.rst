Utility Classes
===============

Classes which implement functionality which is commonly useful for ``ezmsg`` ``Systems``.

MessageGate
-----------

Inherits from ``Unit``. Blocks ``Messages`` from continuing through the system. Can be set as open, closed, open after n messages, or closed after n messages.

Messages
^^^^^^^^

.. py:class:: GateMessage(open: bool)
   
   Send this message to ``INPUT_GATE`` to open or close the gate.

Input/OutputStreams
^^^^^^^^^^^^^^^^^^^

.. object:: INPUT_GATE

   `Type:` ``InputStream(GateMessage)``

   Stop or start message flow. If ``GateMessage.open == True``, messages will flow through. If ``GateMessage.open == False``, messages will be discarded. 

.. object:: INPUT

   `Type:` ``InputStream(Message)``

   Messages which will flow through or be discarded, depending on gate status.

.. object:: OUTPUT

   `Type`: ``OutputStream(Message)``

   Publishes messages which flow through.

Settings
^^^^^^^^

.. py:class:: MessageGateSettings(start_open: bool = False, default_open: bool = False, default_after: Optional[int] = None)

   Inherits from ``Settings``. Define to customize ``MessageGate`` behavior.

   `start_open` sets the gate's initial state to allow messages to flow through or be discarded. ``True`` will allow messages to flow through initially, ``False`` will discard messages initially.

   `default_open` sets the gate's behavior after the `default_after` number of messages have flowed through. ``True`` will allow messages to flow through, ``False`` will discard messages.

   `default_after` sets the number of messages after which the `default_open` state will be applied.

MessageLogger
-------------

Inherits from ``Unit``. Logs all messages it receives to a file. File path can be set in ``SETTINGS`` or set dynamically by passing a `pathlib.Path <https://docs.python.org/3/library/pathlib.html#basic-use>`_ to ``INPUT_START``.

Input/OutputStreams
^^^^^^^^^^^^^^^^^^^

.. object:: INPUT_START

   `Type:` ``InputStream(pathlib.Path)``

   Pass a `pathlib.Path <https://docs.python.org/3/library/pathlib.html#basic-use>`_ to begin logging messages to that path. If the file path already exists, the existing file will be truncated to 0 length. If the file is already open, nothing will happen.

.. object:: INPUT_STOP

   `Type:` ``InputStream(pathlib.Path)``

   Pass a `pathlib.Path <https://docs.python.org/3/library/pathlib.html#basic-use>`_ to stop logging messages to that path.

.. object:: INPUT_MESSAGE

   `Type:` ``InputStream(Any)``

   Pass a piece of data to log it to every open file which the ``MessageLogger`` is using.

.. object:: OUTPUT_MESSAGE

   `Type`: ``OutputStream(Any)``

   Messages which are sent to ``INPUT_MESSAGE`` will pass through and be published on ``OUTPUT_MESSAGE``.

.. object:: OUTPUT_START

   `Type:` ``OutputStream(pathlib.Path)``

   If a file passed to ``INPUT_START`` is successfully opened, its path will be published to ``OUTPUT_START``, otherwise ``None``.

.. object:: OUTPUT_STOP

   `Type:` ``OutputStream(pathlib.Path)``

   If a file passed to ``INPUT_STOP`` is successfully closed, its path will be published to ``OUTPUT_STOP``, otherwise ``None``.


Settings
^^^^^^^^

.. py:class:: MessageLoggerSettings(output: Optional[Path] = None)

   Pass a `pathlib.Path <https://docs.python.org/3/library/pathlib.html#basic-use>`_ for a file where the messages will be logged. If the file path already exists, the existing file will be truncated to 0 length.
