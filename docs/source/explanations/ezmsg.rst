About ezmsg
############

``ezmsg`` is a lightweight, flexible, and extensible framework for building signal processing pipelines. It is designed to facilitate the development, deployment, and management of complex signal processing workflows, making it easier for users to create and share reusable components.


****************************
Description
****************************

providing a flexible and extensible framework for signal processing tasks makes it 
- easier for users to create custom signal processors
- easier for users to integrate with ezmsg and create ezmsg Units 
- easier to create processing pipelines in the ``ezmsg`` ecosystem
- allows standalone use outside of an ezmsg context

Media 
=======

* `BCI Society 2025 Poster <https://github.com/ezmsg-org/ezmsg/releases/download/V3.6.1/ezmsg_poster_BCI2025.pdf>`_
* `Society For Neuroscience 2024 Poster <https://github.com/ezmsg-org/ezmsg/releases/download/V3.6.1/ezmsg_poster_SfN2024.pdf>`_


**********************************************
Modular Design
**********************************************

(include decision tree here)

*********************************
Backend Implementation
*********************************

======================
Graphserver
======================

When an ``ezmsg`` pipeline is started, ``ezmsg`` initializes two additional processes to manage execution. The first is the SharedMemoryServer, which allocates blocks of memory to nodes. The second is the GraphServer, which keeps track of the graph state during execution. One GraphServer should be initialized per ezmsg pipeline, and one SharedMemoryServer should be initialized per machine which the pipeline is running on. Check out the Other page for more information on managing the SharedMemoryServer and GraphServer during pipeline execution.

=======================
Pub/Sub Design
=======================
(fix this crap)
The `ezmsg-sigproc` extension utilizes a pub/sub design pattern, allowing signal processors to subscribe to specific data streams and publish processed results. This design promotes decoupling between components, enabling flexible and scalable signal processing architectures. Users can easily create custom signal processors that fit into the pub/sub model, enhancing the overall functionality of the `ezmsg` framework. 


******************************
Command Line Interface
******************************

The ``ezmsg`` command line interface exposes extra tools to manage a pipeline that is running in the background on a machine. Run 

.. code-block:: python
    ezmsg -h 

to see all the available options.
How to run ezmsg from command Line
include explanation of all command line options

*********************************
Unit components 
*********************************

Basic building block is a unit

=========================
SETTINGS
=========================

=========================
STATE
=========================

=========================
streams
=========================

=========================
methods
=========================

include explanation of decorators and how to use them

*********************************
AxisArray
*********************************

preferred Message format is AxisArray. See `:doc:`AxisArray <axisarray>` for more information.

*********************************
Extensions
*********************************

How to install
How to use


********************************
See Also
********************************

1. `Ezmsg Documentation <ezmsg_documentation>`_
#. :doc: `axisarray`
#. :doc: `sigproc`
#. :doc: `../tutorials/start`
#. :doc: `../how-tos/basics/main`
#. :doc: `../how-tos/pipeline/main`
