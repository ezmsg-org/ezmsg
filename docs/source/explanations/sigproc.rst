About ezmsg-sigproc Signal Processors
###########################################


`ezmsg-sigproc` is an `ezmsg` extension that provides a template for building signal processing classes as well a way to easily convert to ezmsg ``Unit``\ s. 

It also comes with a collection of pre-built signal processing classes and relevant ezmsg Units that accomplish standard signal processing tasks and are designed to work seamlessly within the ``ezmsg`` framework. 

A list of available signal processors and ezmsg Units can be found in the `ezmsg-sigproc reference <../extensions/sigproc/content-sigproc>`_.


|ezmsg_logo_small| Rationale For Implementation
********************************************************

Providing a flexible and extensible framework for signal processing tasks makes it

- easier for users to create custom signal processors
- easier for users to integrate with ezmsg and create ezmsg Units 
- easier to create processing pipelines in the ``ezmsg`` ecosystem
- allows standalone use outside of an ezmsg context


|ezmsg_logo_small| How to decide which processor template to use?
******************************************************************

We use the term "processor" to refer to any class that processes signals. We then separate processors into types based on whether or not they receive input messages (typically signal data), send output messages, or both:

- A producer sends output messages, but does not receive input
- A consumer receives input, but does not output
- A transformer receives input and sends output. 

Furthermore, if a processor of any type must maintain state between processing calls (e.g., filtering, modulation, etc.), it is considered a stateful processor. For example, a producer that is stateful is called a stateful producer. 

Additionally, we also consider adaptive stateful transformers, which are stateful transformers that adapt their internal state based on the input signal characteristics (e.g., adaptive filters). If we would like a transformer to be asynchronous in all calls, we would use an asynchronous transformer.

The decision tree for this classification is as follows:

.. graphviz::
    :align: center

    digraph signal_processor_decision_tree {
        node [shape=box, style="rounded,filled", fillcolor="#f0f0f0", fontname="Arial"];
        edge [fontname="Arial"];

        AMP [label="Multiple Processors?"];
        ARI [label="Receives Input?"];
        ACB [label="Single Chain / Branching?"];
        P [label="Producer", shape=diamond, fillcolor="#27f21cff"];
        APO [label="Produces Output?"];
        NBC [label="no base class", style="none"];
        ACRI [label="Receives Input?"];
        C [label="Consumer", shape=diamond, fillcolor="#27f21cff"];
        T [label="Transformer", shape=diamond, fillcolor="#27f21cff"];
        PS [label="Stateful?"];
        CS [label="Stateful?"];
        TS [label="Stateful?"];
        TSA [label="Adaptive?"];
        TSAF [label="Async First?"];
        CompositeProducer [style="none, filled", fillcolor="#effb1aff"];
        CompositeProcessor [style="none, filled", fillcolor="#effb1aff"];
        BaseProducer [style="none, filled", fillcolor="#effb1aff"];
        BaseStatefulProducer [style="none, filled", fillcolor="#effb1aff"];
        BaseConsumer [style="none, filled", fillcolor="#effb1aff"];
        BaseStatefulConsumer [style="none, filled", fillcolor="#effb1aff"];
        BaseTransformer [style="none, filled", fillcolor="#effb1aff"];
        BaseAdaptiveTransformer [style="none, filled", fillcolor="#effb1aff"];
        BaseStatefulTransformer [style="none, filled", fillcolor="#effb1aff"];
        BaseAsyncTransformer [style="none, filled", fillcolor="#effb1aff"];

        AMP -> ARI [label="no"];
        AMP -> ACB [label="yes"];
        ARI -> P [label="no"];
        ARI -> APO [label="yes"];
        ACB -> NBC [label="branching"];
        ACB -> ACRI [label="single chain"];
        P -> PS;
        APO -> C [label="no"];
        APO -> T [label="yes"];
        ACRI -> CompositeProducer [label="no"];
        ACRI -> CompositeProcessor [label="yes"];
        PS -> BaseProducer [label="no"];
        PS -> BaseStatefulProducer [label="yes"];
        C -> CS;
        T -> TS;
        CS -> BaseConsumer [label="no"];
        CS -> BaseStatefulConsumer [label="yes"];
        TS -> BaseTransformer [label="no"];
        TS -> TSA [label="yes"];
        TSA -> TSAF [label="no"];
        TSA -> BaseAdaptiveTransformer [label="yes"];
        TSAF -> BaseStatefulTransformer [label="no"];
        TSAF -> BaseAsyncTransformer [label="yes"];
    }

The leaf nodes in yellow are abstract base classes provided in `ezmsg.sigproc.base` for implementing standalone processors. The table below summarizes these base classes.

|ezmsg_logo_small| Abstract implementations (Base Classes) for standalone processors
***************************************************************************************


Generic TypeVars
===================

In this table, we summarize the generic TypeVars used in the processor class protocols and abstract base classes provided in `ezmsg.sigproc.base`.

.. list-table:: 
   :widths: 5 20 30
   :header-rows: 1

   * - Idx
     - Class
     - Description
   * - 1
     - `MessageInType` (Mi)
     - for messages passed to a consumer, processor, or transformer
   * - 2
     - `MessageOutType` (Mo)
     - for messages returned by a producer, processor, or transformer
   * - 3
     - `SettingsType`
     - bound to ``ez.Settings``
   * - 4
     - `StateType` (St)
     - bound to ``ProcessorState`` which is simply ``ez.State`` with a ``hash: int`` field.


Processor Class Protocols
===========================
In this table, we summarize the processor class protocols used to define the abstract base classes provided in `ezmsg.sigproc.base`. Each protocol corresponds to a specific processor type and characteristics as outlined in the decision tree above.

+-----+-----------------------+--------+-------+------------------------+--------+-----------------+
| Idx | Class                 | Parent | State | ``__call__`` signature | @state | ``partial_fit`` |
+=====+=======================+========+=======+========================+========+=================+
| 1   | `Processor`           | \-     | No    | Any -> Any             | \-     | \-              |
+-----+-----------------------+--------+-------+------------------------+--------+-----------------+
| 2   | `Producer`            | \-     | No    | None -> Mo             | \-     | \-              |
+-----+-----------------------+--------+-------+------------------------+--------+-----------------+
| 3   | `Consumer`            | 1      | No    | Mi -> None             | \-     | \-              |
+-----+-----------------------+--------+-------+------------------------+--------+-----------------+
| 4   | `Transformer`         | 1      | No    | Mi -> Mo               | \-     | \-              |
+-----+-----------------------+--------+-------+------------------------+--------+-----------------+
| 5   | `StatefulProcessor`   | \-     | Yes   | Any -> Any             | Y      | \-              |
+-----+-----------------------+--------+-------+------------------------+--------+-----------------+
| 6   | `StatefulProducer`    | \-     | Yes   | None -> Mo             | Y      | \-              |
+-----+-----------------------+--------+-------+------------------------+--------+-----------------+
| 7   | `StatefulConsumer`    | 5      | Yes   | Mi -> None             | Y      | \-              |
+-----+-----------------------+--------+-------+------------------------+--------+-----------------+
| 8   | `StatefulTransformer` | 5      | Yes   | Mi -> Mo               | Y      | \-              |
+-----+-----------------------+--------+-------+------------------------+--------+-----------------+
| 9   | `AdaptiveTransformer` | 8      | Yes   | Mi -> Mo               | Y      | Y               |
+-----+-----------------------+--------+-------+------------------------+--------+-----------------+

Note: ``__call__`` and ``partial_fit`` both have asynchronous alternatives: ``__acall__`` and ``apartial_fit`` respectively.


Processor Base Classes
========================

In this table, we summarize the abstract base classes provided in `ezmsg.sigproc.base` for implementing standalone signal processors. Each base class corresponds to a specific processor type and protocol, as outlined in the decision tree above.

.. list-table:: 
   :widths: 5 20 5 5 30
   :header-rows: 1

   * - Idx
     - Class
     - Parent
     - Protocol
     - Features
   * - 1
     - ``BaseProcessor`` 
     - \-
     - 1
     - ``__init__`` for settings; ``__call__`` (alias: ``send``) wraps abstract ``_process``.
   * - 2
     - ``BaseProducer``
     - \-
     - 2
     - Similar to ``BaseProcessor``; ``next``/``anext`` instead of ``send``/``asend`` aliases. async first!
   * - 3
     - ``BaseConsumer``
     - 1
     - 3
     - Overrides return type to None.
   * - 4
     - ``BaseTransformer``
     - 1
     - 4
     - Overrides input and return types.
   * - 5
     - ``BaseStatefulProcessor``
     - 1
     - 5
     - ``state`` setter unpickles arg; ``stateful_op`` wraps ``__call__``.
   * - 6
     - ``BaseStatefulProducer``
     - 2
     - 6
     - ``state`` setter and getter; ``stateful_op`` wraps ``__call__`` which runs ``__acall__``.
   * - 7
     - ``BaseStatefulConsumer``
     - 5
     - 7
     - Overrides return type to None.
   * - 8
     - ``BaseStatefulTransformer``
     - 5
     - 8
     - Overrides input and return types.
   * - 9
     - ``BaseAdaptiveTransformer``
     - 8
     - 9
     - Implements ``partial_fit``. ``__call__`` may call ``partial_fit`` if message has ``.trigger``.
   * - 10
     - ``BaseAsyncTransformer``
     - 8
     - 8
     - ``__acall__`` wraps abstract ``_aprocess``; ``__call__`` runs ``__acall__``.
   * - 11
     - ``CompositeProcessor``
     - 1
     - 5
     - Methods iterate over sequence of processors created in ``_initialize_processors``.
   * - 12
     - ``CompositeProducer``
     - 2
     - 6
     - Similar to ``CompositeProcessor``, but first processor must be a producer.

NOTES:

1. Producers do not inherit from ``BaseProcessor``, so concrete implementations should subclass ``BaseProducer`` or ``BaseStatefulProducer``.
2. For concrete implementations of non-producer processors, inherit from the base subclasses of ``BaseProcessor`` (eg. ``BaseConsumer``, ``BaseTransformer``) and from base subclasses of ``BaseStatefulProcessor``. These two processor classes are primarily used for efficient abstract base class construction.
3. For most base classes, the async methods simply call the synchronous methods where the processor logic is expected. Exceptions are ``BaseProducer`` (and its children) and ``BaseAsyncTransformer`` which are async-first and should be strongly considered for operations that are I/O bound.
4. For async-first classes, the logic is implemented in the async methods and the sync methods are thin wrappers around the async methods. The wrapper uses a helper method called ``run_coroutine_sync`` to run the async method in a synchronous context, but this adds some noticeable processing overhead.
5. If you need to call your processor outside ezmsg (which uses async), and you cannot easily add an async context* in your processing, then you might want to consider duplicating the processor logic in the sync methods. 

  .. note:: Jupyter notebooks are async by default, so you can await async code in a notebook without any extra setup.

6. ``CompositeProcessor`` and ``CompositeProducer`` are stateful, and structurally subclass the ``StatefulProcessor`` and ``StatefulProducer`` protocols, but they
do not inherit from ``BaseStatefulProcessor`` and ``BaseStatefulProducer``. They accomplish statefulness by inheriting from the mixin abstract base class ``CompositeStateful``, which implements the state related methods: ``get_state_type``, ``state.setter``, ``state.getter``, ``_hash_message``, ``_reset_state``, and ``stateful_op`` (as well as composite processor chain related methods). However, ``BaseStatefulProcessor``, ``BaseStatefulProducer`` implement ``stateful_op`` method for a single processor in an incompatible way to what is required for composite chains of processors.


|ezmsg_logo_small| Implementing a custom standalone processor
****************************************************************

1. Create a new settings dataclass: ``class MySettings(ez.Settings):``
2. Create a new state dataclass:

.. code-block:: python

  @processor_state
  class MyState:

3. Decide on your base processor class, considering the protocol, whether it should be async-first, and other factors using the decision tree above. 

4. Implement the child class.
    * The minimum implementation is ``_process`` for sync processors, ``_aprocess`` for async processors, and ``_produce`` for producers.
    * For any stateful processor, implement ``_reset_state``.
    * For stateful processors that need to respond to a change in the incoming data, implement ``_hash_message``.
    * For adaptive transformers, implement ``partial_fit``.
    * For chains of processors (``CompositeProcessor``/ ``CompositeProducer``), need to implement ``_initialize_processors``.
    * See processors in `ezmsg.sigproc` for examples.
5. Override non-abstract methods if you need special behaviour. For example:
    * ``WindowTransformer`` overrides ``__init__`` to do some sanity checks on the provided settings.
    * ``TransposeTransformer`` and ``WindowTransformer`` override ``__call__`` to provide a passthrough shortcut when the settings allow for it.
    * ``ClockProducer`` overrides ``__call__`` in order to provide a synchronous call bypassing the default async behaviour.


|ezmsg_logo_small| Abstract implementations (Base Classes) for ezmsg Units using processors
**********************************************************************************************

Generic TypeVars for ezmsg Units
==================================

.. list-table:: 
   :widths: 5 20 30
   :header-rows: 1

   * - Idx
     - Class
     - Description
   * - 5
     - ``ProducerType`` 
     - bound to ``BaseProducer`` (hence, also ``BaseStatefulProducer``, ``CompositeProducer``)
   * - 6
     - ``ConsumerType`` 
     - bound to ``BaseConsumer``, ``BaseStatefulConsumer``
   * - 7
     - ``TransformerType``
     - bound to ``BaseTransformer``, ``BaseStatefulTransformer``, ``CompositeProcessor`` (hence, also ``BaseAsyncTransformer``)
   * - 8
     - ``AdaptiveTransformerType``
     - bound to ``BaseAdaptiveTransformer``


Base Classes for ezmsg processor Units:
==============================================================================

+-----+---------------------------------+---------+-----------------------------+
| Idx | Class                           | Parents | Expected TypeVars           |
+=====+=================================+=========+=============================+
| 1   | ``BaseProcessorUnit``           | \-      | \-                          |
+-----+---------------------------------+---------+-----------------------------+
| 2   | ``BaseProducerUnit``            | \-      | ``ProducerType``            |
+-----+---------------------------------+---------+-----------------------------+
| 3   | ``BaseConsumerUnit``            | 1       | ``ConsumerType``            |
+-----+---------------------------------+---------+-----------------------------+
| 4   | ``BaseTransformerUnit``         | 1       | ``TransformerType``         |
+-----+---------------------------------+---------+-----------------------------+
| 5   | ``BaseAdaptiveTransformerUnit`` | 1       | ``AdaptiveTransformerType`` |
+-----+---------------------------------+---------+-----------------------------+

Note, it is strongly recommended to use `BaseConsumerUnit`, `BaseTransformerUnit`, or `BaseAdaptiveTransformerUnit` for implementing concrete subclasses rather than `BaseProcessorUnit`.

|ezmsg_logo_small| How to implement a custom ezmsg Unit from a standalone processor
=====================================================================================

1. Create and test custom standalone processor as above.
2. Decide which base unit to implement.
    * Use the "Generic TypeVars for ezmsg Units" table above to determine the expected TypeVar.
    * Find the Expected TypeVar in the "ezmsg Units" table.
3. Create the derived class.

Often, all that is required is the following (e.g., for a custom transformer):

.. code-block:: python

  import ezmsg.core as ez
  from ezmsg.util.messages.axisarray import AxisArray
  from ezmsg.sigproc.base import BaseTransformer, BaseTransformerUnit


  class CustomTransformerSettings(ez.Settings):
      ...


  class CustomTransformer(BaseTransformer[CustomTransformerSettings, AxisArray, AxisArray]):
      def _process(self, message: AxisArray) -> AxisArray:
          # Your processing code here...
          return message


  class CustomUnit(BaseTransformerUnit[
          CustomTransformerSettings,    # SettingsType
          AxisArray,                    # MessageInType
          AxisArray,                    # MessageOutType
          CustomTransformer,            # TransformerType
      ]):
          SETTINGS = CustomTransformerSettings


.. note:: The type of ProcessorUnit is based on the internal processor and not the input or output of the unit. Input streams are allowed in ProducerUnits and output streams in ConsumerUnits. For an example of such a use case, see ``BaseCounterFirstProducerUnit`` and its subclasses. ``BaseCounterFirstProducerUnit`` has an input stream that receives a flag signal from a clock that triggers a call to the internal producer.

|ezmsg_logo_small| See Also
********************************

1. `Signal Processor Documentation <sigproc_processor_documentation>`_
#. `Signal Processing Tutorial <../../tutorials/signalprocessing.html>`_
#. `Signal Processing HOW TOs <../../how-tos/signalprocessing/main.html>`_

.. |ezmsg_logo_small| image:: ../_static/_images/ezmsg_logo.png
  :width: 40
  :alt: ezmsg logo