API
===

An ``ezmsg`` system is created from a few basic components. ``ezmsg`` provides a framework for you to define your own graphs using its building blocks. Inherit from its base components to define a pipeline that works for your project.


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

Component
---------

Metaclass which ``Unit``s and ``Collection``s inherit from.

Complete
--------

.. py:class:: Complete

   A type of ``Exception`` which signals to ``ezmsg`` that the function can be shut down gracefully. If all functions in all ``Units`` raise ``Complete``, the entire ``System`` will terminate execution.


NetworkDefinition
------------------

.. py:class:: NetworkDefinition

   Wrapper on ``Iterator[Tuple[OutputStream, InputStream]]``.

.. _run-system:

NormalTermination
-----------------

.. py:class:: NormalTermination

   A type of ``Exception`` which signals to ``ezmsg`` that the ``System`` can be shut down gracefully. 

run
---

.. py:method:: run(components: {str: Component} = None,
    root_name: str = None,
    connections: NetworkDefinition = None,
    process_components: [Component] = None,
    backend_process: BackendProcess = DefaultBackendProcess,
    graph_address: (str, int) = None,
    force_single_process: bool = False,
    **components_kwargs: Component)
()

   `The old method` run_system() `has been deprecated and uses` run() `instead.`

   Begin execution of a set of ``Component``s.

   `components` represents the nodes in the directed acyclic graph. It is a dictionary which contains the ``Component``s to be run mapped to string names. On initialization, ``ezmsg`` will call ``initialize()`` for each ``Unit`` and ``configure()`` for each ``Collection``, if defined.

   `connections` represents the edges is a ``NetworkDefinition`` which connects ``OutputStream``s to ``InputStreams``. On initialization, ``ezmsg`` will create a directed acyclic graph using the contents of this parameter.

   `process_components` is a list of ``Component``s which should live in their own process.

   `backend_process` is currently under development.

   `graph_address` is a tuple which contains the hostname and port of the graph server which ``ezmsg`` should connect to. If not defined, ``ezmsg`` will start a new graph server at 127.0.0.1:25978. 

   `force_single_process` will run all ``Component``s in one process. This is necessary when running ``ezmsg`` in a notebook.

Settings
--------

To pass parameters into a ``Component``, inherit from ``Settings``.

.. code-block:: python

   class YourSettings(Settings): 
      setting1: int
      setting2: float

To use, declare the ``Settings`` object for a ``Component`` as a member variable called (all-caps!) ``SETTINGS``. ``ezmsg`` will monitor the variable called ``SETTINGS`` in the background, so it is important to name it correctly.

.. code-block:: python

   class YourUnit(Unit):

      SETTINGS: YourSettings

A ``Unit`` can accept a ``Settings`` object as a parameter on instantiation.

.. code-block:: python

   class YourCollection(Collection):

      YOUR_UNIT = YourUnit(
         YourSettings(
            setting1: int,
            setting2: float
         )
      )

.. note:: 
   ``Settings`` uses type hints to define member variables, but does not enforce type checking.

State
-----

To track a mutable state for a ``Component``, inherit from ``State``.

.. code-block:: python

   class YourState(State):
      state1: int
      state2: float

To use, declare the ``State`` object for a ``Component`` as a member variable called (all-caps!) ``STATE``. ``ezmsg`` will monitor the variable called ``STATE`` in the background, so it is important to name it correctly.

Member functions can then access and mutate ``STATE`` as needed during function execution. It is recommended to initialize state values inside the ``initialize()`` or ``configure()`` lifecycle hooks if defaults are not defined.

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

Facilitates a flow of ``Messages`` into or out of a ``Component``. 

.. class:: InputStream(Message)

   Can be added to any ``Component`` as a member variable. Methods may subscribe to it.


.. class:: OutputStream(Message)

   Can be added to any ``Component`` as a member variable. Methods may publish to it.


Unit
----

Represents a single step in the graph. To create a ``Unit``, inherit from the ``Unit`` class.

Lifecycle Hooks
^^^^^^^^^^^^^^^

The following lifecycle hooks in the ``Unit`` class can be overridden. Both can be run as ``async`` functions by simply adding the ``async`` keyword when overriding.

.. py:method:: initialize( self ) 

   Runs when the ``Unit`` is instantiated.

.. py:method:: shutdown( self )

   Runs when the ``Unit`` terminates.

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
