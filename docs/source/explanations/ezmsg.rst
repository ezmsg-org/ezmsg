About `ezmsg`
###############

(under construction - Griffin to add content)

`ezmsg` is a lightweight, flexible, and extensible framework for building signal processing pipelines. It is designed to facilitate the development, deployment, and management of complex signal processing workflows, making it easier for users to create and share reusable components.


|ezmsg_logo_small| Description
****************************

Providing a flexible and extensible framework for signal processing tasks makes it 

- easier for users to create custom signal processors
- easier for users to integrate with ezmsg and create ezmsg Units 
- easier to create processing pipelines in the `ezmsg` ecosystem
- allows standalone use outside of an ezmsg context

Media 
=======

* `BCI Society 2025 Poster <https://github.com/ezmsg-org/ezmsg/releases/download/V3.6.1/ezmsg_poster_BCI2025.pdf>`_
* `Society For Neuroscience 2024 Poster <https://github.com/ezmsg-org/ezmsg/releases/download/V3.6.1/ezmsg_poster_SfN2024.pdf>`_



|ezmsg_logo_small| Modular Design
**********************************************

(include example diagram here)

`ezmsg` is built around a modular design philosophy, where individual components (called `Units`) can be developed, tested, and reused independently. This modularity allows users to easily assemble and rearrange complex processing pipelines by connecting different Units together.


|ezmsg_logo_small| Backend Implementation
******************************************

(include diagram of architecture here)

(maybe include detailed process order diagram here)


Graphserver
======================

When an `ezmsg` pipeline is started, `ezmsg` initializes two additional processes to manage execution. The first is the SharedMemoryServer, which allocates blocks of memory to nodes. The second is the GraphServer, which keeps track of the graph state during execution. One GraphServer should be initialized per ezmsg pipeline. 


Pub/Sub Design
=======================
(fix this crap)
The `ezmsg-sigproc` extension utilizes a pub/sub design pattern, allowing signal processors to subscribe to specific data streams and publish processed results. This design promotes decoupling between components, enabling flexible and scalable signal processing architectures. Users can easily create custom signal processors that fit into the pub/sub model, enhancing the overall functionality of the `ezmsg` framework. 



|ezmsg_logo_small| Command Line Interface
*******************************************

The `ezmsg` command line interface exposes extra tools to manage a pipeline that is running in the background on a machine. Run 

.. code-block:: python

    ezmsg -h 

to see all the available options.


Basic ezmsg building blocks
*********************************

Basic building block in `ezmsg` is a `Unit`, which represents a discrete processing element within a pipeline. Users can create custom Units by subclassing the base Unit class and implementing the required processing logic. One can combine multiple `Unit`\ s to form a `Collection`, which functions much like a `Unit` does (one may want to abstract away complexity by having a `Collection` representing a logical grouping of `Unit`\ s). The following discussion applies as much to `Collection`\ s as it does to `Unit`\ s, so we will just refer to `Unit`\ s for simplicity.

A `Unit` typically contains the following attributes/components:

- ``SETTINGS``: Configuration parameters that define the behavior of the `Unit`. These can be set during initialization or modified at runtime - though they are usually chosen for the lifetime of the `Unit`. 
- ``STATE``: Internal state variables that maintain the current status of the `Unit`. These can be updated during processing to reflect changes in the `Unit`\ s operation. Unlike the parameters in ``SETTINGS``, these are expected to change frequently during the lifetime of the `Unit`.
- input and output streams: Data channels through which the `Unit` receives input and sends output. These streams facilitate communication between different `Unit`\ s in a pipeline.
- processing methods: Functions that define the core processing logic of the `Unit`. These methods can be decorated to be invoked when data is received on the input streams, and produce output that is sent to the output streams.


SETTINGS
=========================

This attribute is to be declared in the `Unit` in the format:

.. code-block:: python

   SETTINGS = RelevantSettingsClass

The capitalization is important as ezmsg reserves this attribute name for this purpose and this is critical for the backend implementation of the `Unit`. Notice that we do not instantiate the settings class here, we just provide a reference to the class. ezmsg will take care of instantiating the settings class when the pipeline is created or in some cases when it receives the first message. There must be at most one such attribute in a `Unit` or `Collection`.

STATE
=========================

This attribute is to be declared in the `Unit` in the format:

.. code-block:: python

   STATE = RelevantStateClass

As with ``SETTINGS``, the capitalization here is important as ezmsg reserves this attribute name for this purpose and this is critical for the backend implementation of the `Unit`. Notice that we do not instantiate the state class here, we just provide a reference to the class. ezmsg will take care of instantiating the state class when the pipeline is created or in some cases when it receives the first message. There must be at most one such attribute in a `Unit` or `Collection`.

streams
=========================

A unit must have at least one input or output stream. Streams are defined as class attributes in the `Unit` in the format:

.. code-block:: python

   INPUT = ez.InputStream(MessageInType)
   OUTPUT = ez.OutputStream(MessageOutType)

.. note:: ``ez`` here refers to the typical import alias for ezmsg, i.e. ``import ezmsg.core as ez``

Unlike with ``SETTINGS`` and ``STATE``, the capitalization of the stream names and the names in fact are not reserved, though we recommend using something understandable. One can have as many input and output streams as needed in a `Unit` or `Collection`. The message types can be any type, and for signal processing purposes, we recommend our own implemented message type :doc:`AxisArray <axisarray>`.

methods
=========================

include explanation of decorators and how to use them


|ezmsg_logo_small| AxisArray
*********************************

Preferred Message format is AxisArray. See `:doc:`AxisArray <axisarray>` for more information.


|ezmsg_logo_small| Extensions
*********************************

`ezmsg` extensions can be installed individually or all at once. To install all the extension packages in one go, you can use the following command:

.. code-block:: bash

    pip install "ezmsg[all_ext]"


This will install all the available public extension packages for `ezmsg` that are listed in `pyproject.toml`.
If you prefer to install a subset of extension packages, you can use the following command:

.. code-block:: bash

    pip install "ezmsg[zmq,sigproc,...]"

Please note that the `ezmsg` package itself can still be installed without any additional extensions using `pip install ezmsg`.

Extensions can be managed manually as well. Here are some of the extensions we manage or are aware of:

- `ezmsg-sigproc <https://github.com/ezmsg-org/ezmsg-sigproc>`_ -- Timeseries signal processing modules
- `ezmsg-websocket <https://github.com/ezmsg-org/ezmsg-websocket>`_ -- Websocket server and client nodes for `ezmsg` graphs
- `ezmsg-zmq <https://github.com/ezmsg-org/ezmsg-zmq>`_ -- ZeroMQ pub and sub nodes for `ezmsg` graphs
- `ezmsg-panel <https://github.com/griffinmilsap/ezmsg-panel>`_ -- Plotting tools for `ezmsg` that use `panel <https://github.com/holoviz/panel>`_
- `ezmsg-blackrock <https://github.com/griffinmilsap/ezmsg-blackrock>`_ -- Interface for Blackrock Cerebus ecosystem (incl. Neuroport) using `pycbsdk`
- `ezmsg-lsl <https://github.com/ezmsg-org/ezmsg-lsl>`_ -- Source unit for LSL Inlet and sink unit for LSL Outlet
- `ezmsg-unicorn <https://github.com/griffinmilsap/ezmsg-unicorn>`_ -- g.tec Unicorn Hybrid Black integration for `ezmsg`
- `ezmsg-gadget <https://github.com/griffinmilsap/ezmsg-gadget>`_ -- USB-gadget with HID control integration for Raspberry Pi (Zero/W/2W, 4, CM4)
- `ezmsg-openbci <https://github.com/griffinmilsap/ezmsg-openbci>`_ -- OpenBCI Cyton serial interface for `ezmsg`
- `ezmsg-ssvep <https://github.com/griffinmilsap/ezmsg-ssvep>`_ -- Tools for running SSVEP experiments with `ezmsg`
- `ezmsg-vispy <https://github.com/pperanich/ezmsg-vispy>`_ -- `ezmsg` visualization toolkit using PyQt6 and vispy.

See :doc:`Extensions page <../extensions/content-extensions>` for more information.



|ezmsg_logo_small| See Also
********************************

#. :doc:`axisarray`
#. :doc:`sigproc`
#. :doc:`../tutorials/start`
#. :doc:`../how-tos/basics/content-basics`
#. :doc:`../how-tos/pipeline/content-pipeline`

.. |ezmsg_logo_small| image:: ../_static/_images/ezmsg_logo.png
  :width: 40
  :alt: ezmsg logo