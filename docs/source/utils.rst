Utility Classes
===============

Classes which implement functionality which is commonly useful for ``ezmsg`` pipelines.

DebugLog
--------

Inherits from ``Units``. Logs messages that pass through.

Input/OutputStreams
^^^^^^^^^^^^^^^^^^^

.. object:: INPUT

   `Type:` ``InputStream(Any)``

   Send messages to log here.

.. object:: OUTPUT

   `Type:` ``OutputStream(Any)``

   Send messages back out to continue through the graph.

Settings
^^^^^^^^

.. py:class:: DebugLogSettings(name: str = "DEBUG", max_length: Optional[int] = 400)

   Inherits from ``Settings``. Define to customize.

   `name` is included in the logstring so that if multiple DebugLogs are used in one pipeline, their messages can be differentiated.

   `max_length` sets a maximum number of chars which will be printed from the message. If the message is longer, the log message will be truncated.

Message Collector
-----------------

Inherits from ``Unit``. Collects ``Messages`` into a local list.

Input/OutputStreams
^^^^^^^^^^^^^^^^^^^

.. object:: INPUT_MESSAGE

   Send messages here to be collected.

.. object:: OUTPUT_MESSAGE

   Messages will pass straight through after being recorded and be published here.

Methods
^^^^^^^

.. py:method:: messages() -> [Any]

   Returns a list of messages which have been collected.


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

   `Type:` ``InputStream(Any)``

   Messages which will flow through or be discarded, depending on gate status.

.. object:: OUTPUT

   `Type`: ``OutputStream(Any)``

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

MessageQueue
------------

Inherits from ``Unit``. Place between two other ``Units`` to induce backpressure.

Input/OutputStreams
^^^^^^^^^^^^^^^^^^^

.. object:: INPUT

   `Type:` ``InputStream(Any)``

   Send messages to queue here.

.. object:: OUTPUT

   `Type:` ``OutputStream(Any)``

   Subscribe to pull messages out of the queue.

Settings
^^^^^^^^

.. py:class:: MessageQueueSettings(maxsize: int = 0, leaky: bool = False)

`maxsize` indicates the maximum number of items which the queue will hold.

`leaky` indicates whether the queue will drop new messages when it reaches its maxsize, or whether it will wait for space to open for them.

MessageReplay
-------------

Inherits from ``Unit``. Stream messages from files created by ``MessageLogger``. Stores a queue of files to stream and streams from them in order.

Messages
^^^^^^^^
.. py:class:: ReplayStatusMessage(filename: Path, idx: int, total: int, done: bool = False)

   Message which gives the status of a file replay.

   `filename` is the file currently being replayed.

   `idx` is the line number of the message that was just published.

   `total` is the total number of messages in the file.

   `done` denotes whether the file has finished replaying. 

.. py:class:: FileReplayMessage(filename: Optional[Path] = None, rate: Optional[float] = None)

   Add a file to the queue.

   `filename` is the path of the file to replay.

   `rate` in Hertz at which the messages will be published. If not specified, messages will publish as fast as possible.

Input/OutputStreams
^^^^^^^^^^^^^^^^^^^

.. object:: INPUT_FILE
   
   `Type:` ``InputStream(FileReplayMessage)``

   Add a new file to the queue.

.. object:: INPUT_PAUSED
   
   `Type:` ``InputStream(bool)``

   Send ``True`` to pause the stream, ``False`` to restart the stream.

.. object:: INPUT_STOP
   
   `Type:` ``InputStream(bool)``

   Stop the stream. Send ``True`` to also clear the queue. Send ``False`` to reset to the beginning of the current file.

.. object:: OUTPUT_MESSAGE

   `Type:` ``OutputStream(Any)``

   The output on which the messages from the files will be streamed.

.. object:: OUTPUT_TOTAL
   
   `Type:` ``OutputStream(int)``

   Publishes an integer total of messages which have been published on OUTPUT_MESSAGE from a single file. Resets when a file completes.

.. object:: OUTPUT_REPLAY_STATUS

   `Type:` ``OutputStream(ReplayStatusMessage)``

   Publishes status messages.

Settings
^^^^^^^^

.. py:class:: MessageReplaySettings(filename: Optional[Path] = None, rate: Optional[float] = None, progress: bool = False):

   `filename` is the path of the file to replay.

   `rate` in Hertz at which the messages will be published. If not specified, messages will publish as fast as possible.

   `progress` will use tqdm to indicate progress through the file. Tqdm must be installed.

TerminateOnTimeout
------------------

End a pipeline execution when a certain amount of time has passed without receiving a message.

Input/OutputStreams
^^^^^^^^^^^^^^^^^^^

.. object:: INPUT

   Send messages here.

Settings
^^^^^^^^

.. py:class:: TerminateOnTimeoutSettings(time: float = 2.0, poll_rate: float = 4.0)

   `time` in seconds after which the pipeline will be terminated if no messages have been received

   `poll_rate`


TerminateOnTotal
----------------

End a pipeline execution once a certain number of messages have been received.

Input/OutputStreams
^^^^^^^^^^^^^^^^^^^

.. object:: INPUT_MESSAGE

   `Type:` ``InputStream(Any)``

   Send messages here.

.. object:: INPUT_TOTAL

   `Type:` ``InputStream(int)``

   Change the total number of messages to terminate after. If this number has already been reached, termination will occur immediately.

Settings
^^^^^^^^

.. py:class:: TerminateOnTotalSettings(total: int = None)

   `total` represents the total number of messages to terminate after