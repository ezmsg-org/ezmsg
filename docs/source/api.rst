API
===

An ``ezmsg`` system is created from a few basic components. ``ezmsg`` provides a framework for you to define your own graphs using its building blocks. Inherit from its base components to define a pipeline that works for your project.

.. TODO: add figure showing how components work together

Collection
----------

Connects ``Units`` together by defining a graph which connects ``OutputStreams`` to ``InputStreams``.

Lifecycle Hooks
^^^^^^^^^^^^^^^

The following lifecycle hooks in the ``Collection`` class can be overridden:

.. py:method:: configure( self )

   Runs when the ``Collection`` is instantiated. This is the best place to call ``Unit.apply_settings()`` on each member ``Unit`` of the ``Collection``.

Overridable Methods
^^^^^^^^^^^^^^^^^^^^

.. py:method:: network( self ) -> NetworkDefinition

   In this method, define and return a ``NetworkDefinition`` which defines how ``InputStreams`` and ``OutputStreams`` from member ``Units`` will be connected.

.. py:method:: process_components( self ) -> Tuple[Unit|Collection, ...]

   In this method, define and return a tuple which contains ``Units`` and ``Collections`` which should run in their own processes.

Complete
--------

.. py:class:: Complete

   A type of ``Exception`` which signals to ``ezmsg`` that the function can be shut down gracefully. If all functions in all ``Units`` raise ``Complete``, the entire ``System`` will terminate execution.

Message
-------

`Soon to be deprecated.`

To define unique data types, inherit from ``Message``.

.. code-block:: python

   class YourMessage(Message):
      attr1: int
      attr2: float

.. note:: 
   ``Message`` uses type hints to define member variables, but does not enforce type checking.

NetworkDefinition
------------------

.. py:class:: NetworkDefinition

   Wrapper on ``Iterator[Tuple[OutputStream, InputStream]]``.

.. _run-system:

NormalTermination
-----------------

.. py:class:: NormalTermination

   A type of ``Exception`` which signals to ``ezmsg`` that the ``System`` can be shut down gracefully. 

run_system
----------

.. py:method:: run_system(system: System, num_buffers: int = 32, init_buf_size: int = 2**16, backend_process: BackendProcess=None)

   Begin execution of a ``System``.

   `system` is the ``System`` that should be started.

   `num_buffers` is the number of blocks of shared memory that the ``System`` will be allocated upon startup. These shared memory blocks will be used to pass messages from ``OutputStreams`` to ``InputStreams``.

   `init_buf_size` is the size in bytes of each block of shared memory that the ``System`` will be allocated upon startup. These shared memory blocks will be used to pass messages from ``OutputStreams`` to ``InputStreams``.

   `backend_process` is currently under development.

Settings
--------

To pass parameters into a ``Unit``, ``Collection``, or ``System``, inherit from ``Settings``.

.. code-block:: python

   class YourSettings(Settings): 
      setting1: int
      setting2: float

To use, declare the ``Settings`` object for a ``Unit`` as a member variable called (all-caps!) ``SETTINGS``. ``ezmsg`` will monitor the variable called ``SETTINGS`` in the background, so it is important to name it correctly.

.. code-block:: python

   class YourUnit(Unit):

      SETTINGS: YourSettings

Instantiate the ``Settings`` object in the ``Collection`` or ``System`` which will hold the ``Unit``. It is recommended to pass the instantiated ``Settings`` object to its ``Unit`` inside the ``configure()`` lifecycle hook.

.. code-block:: python

   class YourSystem(System):

      YOUR_UNIT = YourUnit()

      def configure():
         YOUR_UNIT.apply_settings(YourSettings(
            setting1: int,
            setting2: float
         ))

.. note:: 
   ``Settings`` uses type hints to define member variables, but does not enforce type checking.

State
-----

To track a mutable state for a ``Unit``, ``Collection``, or ``System``, inherit from ``State``.

.. code-block:: python

   class YourState(State):
      state1: int
      state2: float

To use, declare the ``State`` object for a ``Unit`` as a member variable called (all-caps!) ``STATE``. ``ezmsg`` will monitor the variable called ``STATE`` in the background, so it is important to name it correctly.

Member functions can then access and mutate ``STATE`` as needed during function execution. It is recommended to initialize state values inside the ``initialize()`` lifecycle hook if defaults are not defined.

.. code-block:: python

   class YourUnit(Unit):

      STATE: YourState

      def initialize(self):
         this.STATE.state1 = 0
         this.STATE.state2 = 0.0

.. note:: 
   ``State`` uses type hints to define member variables, but does not enforce type checking.

Stream
------

Facilitates a flow of ``Messages`` into or out of a ``Unit`` or ``Collection``. 

.. class:: InputStream(Message)

   Can be added to any ``Unit`` or ``Collection`` as a member variable. Methods may subscribe to it.


.. class:: OutputStream(Message)

   Can be added to any ``Unit`` or ``Collection`` as a member variable. Methods may publish to it.

System
------

A type of ``Collection`` which represents an entire ``ezmsg`` graph. ``Systems`` have no input or output streams and are runnable via :ref:`run-system`.

Lifecycle Hooks
^^^^^^^^^^^^^^^

The following lifecycle hooks for ``System`` can be overridden:

.. py:method:: configure( self )

   Runs when the ``System`` is instantiated. This is the best place to call ``Unit.apply_settings()`` on each member ``Unit`` of the ``System``.

Overridable Methods
^^^^^^^^^^^^^^^^^^^^

.. py:method:: network( self ) -> NetworkDefinition

   In this method, define and return a ``NetworkDefinition`` which defines how ``InputStreams`` and ``OutputStreams`` from member ``Units`` will be connected.

.. py:method:: process_components( self ) -> Tuple[Unit|Collection, ...]

   In this method, define and return a tuple which contains ``Units`` and ``Collections`` which should run in their own processes.

Unit
----

Represents a single step in the graph. To create a ``Unit``, inherit from the ``Unit`` class.

Lifecycle Hooks
^^^^^^^^^^^^^^^

The following lifecycle hooks in the ``Unit`` class can be overridden:

.. py:method:: initialize( self ) 

   Runs when the ``Unit`` is instantiated.

.. py:method:: shutdown( self )

   Runs when the ``System`` terminates.

Function Decorators
^^^^^^^^^^^^^^^^^^^

These function decorators can be added to member functions.

.. py:method:: @subscriber(InputStream)

   An async function will run once per message received from the ``InputStream`` it subscribes to. Example:

   .. code-block:: python

      INPUT = ez.InputStream(Message)

      @subscriber(INPUT)
      async def print_message(self, message: Message) -> None:
         print(message)
   
   A function can have both ``@subscriber`` and ``@publisher`` decorators.

.. py:method:: @publisher(OutputStream)

   An async function will yield messages on the designated ``OutputStream``.

   .. code-block:: python

      from typing import AsyncGenerator

      OUTPUT = OutputStream(ez.Message)

      @publisher(OUTPUT)
      async def send_message(self) -> AsyncGenerator:
         message = Message()
         yield(OUTPUT, message)

   A function can have both ``@subscriber`` and ``@publisher`` decorators.

.. py:method:: @main

   Designates this function to run as the main thread for this ``Unit``. A ``Unit`` may only have one of these.

.. py:method:: @thread

   Designates this function to run as a background thread for this ``Unit``.

.. py:method:: @task 

   Designates this function to run as a task in the task/messaging thread.

.. py:method:: @process

   Designates this function to run in its own process.

.. py:method:: @timeit

   ``ezmsg`` will log the amount of time this function takes to execute.

Public Methods
^^^^^^^^^^^^^^

A class which inherits from ``Unit`` also inherits one public method:

.. function:: Unit.apply_settings( self, settings: Settings )

   Update a ``Unit`` 's ``Settings`` object.
