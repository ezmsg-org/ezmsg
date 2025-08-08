What is ezmsg?
#####################################

.. under construction - Griffin to add content

This section of the documentation aims to provide a comprehensive overview of the `ezmsg` framework from a design and decision-making perspective. These are four "explainer" documents for users to gain an understanding of the major parts of `ezmsg` and why they are implemented the way that they are. This page provides a high-level overview of the framework, its design philosophy, and motivations. The remaining pages delve into the design details of `ezmsg` core components, the in-built message format `AxisArray` and the signal processing tools provided in the `ezmsg-sigproc` extension to `ezmsg` respectively.

.. toctree::
    :maxdepth: 1

    ezmsg-design
    axisarray
    sigproc

Other ways to learn about `ezmsg` include following our :doc:`Tutorial <../tutorials/content-tutorials>`, checking the list of :doc:`HOW TO pages <../how-tos/content-howtos>` and the :doc:`reference documentation <../reference/content-reference>`. 


|ezmsg_logo_small| Description
*******************************

`ezmsg` is a lightweight, flexible, and extensible framework for building signal processing pipelines and message passing. It has the following characteristics:

- **Modular** design: `ezmsg` is built around a modular design philosophy, where individual components (called `Units`) can be developed, tested, and reused independently. This modularity allows users to easily assemble and rearrange complex processing pipelines by connecting different Units together.
- **Pub/sub messaging**: `ezmsg` utilizes a publish/subscribe messaging pattern. This pattern allows components that output (`publishers`) to simultaneously publish to multiple components that receive input (`subscribers`) and vice versa: subscribers to be simultaneously able to receive from multiple publishers. 
- **Asynchronous processing**: `ezmsg` supports asynchronous processing, allowing components to operate independently and in parallel. In effect, the pipeline will not be blocked by a slow component or device that isn't ready to provide data, as the asynchronous control allows the pipeline to move on and return when it is ready. 
- **Multi-process/multi-machine** capability: `ezmsg` is designed to support multi-process and multi-machine configurations, enabling distributed processing and scalability. This allows for greater flexibility in designing signal processing hardware architectures. 
- Efficient **message communication protocols**: `ezmsg` employs efficient message communication protocols to minimize latency and maximize throughput. Unless otherwise specified, publishers and subscribers in the same process communicate using the local message cache. If publishers and subscribers are in different processes, `ezmsg` leverages Python's fast `shared memory` capabilities to facilitate communication. In a context where neither local, nor shared memory communication is possible, `ezmsg` leverages TCP for communication. 
- Written in **Python**: `ezmsg` is implemented in Python, making it usable on a wide range of platforms, and hence, accessible and easy to use for a wide range of users, from researchers to engineers. Other oft-spoken benefits of Python are its simplicity and readability, and heavy adoption within the machine learning community.
- **Minimal boilerplate code** required for `ezmsg` components: `ezmsg` is designed to minimize the amount of boilerplate code required to create new components. This allows users to focus on the core functionality of their components rather than getting bogged down in implementation details.
- Provides an **in-built message format** (`AxisArray`): `AxisArray` is a message format for handling multi-dimensional arrays with labeled axes. It is designed to facilitate the organization, manipulation, and analysis of complex data structures commonly encountered in signal processing and related fields. See :doc:`AxisArray <axisarray>` for more information.
- Provides a **command line interface**: `ezmsg` includes a command line interface (CLI) that allows users to manage and interact with signal processing pipelines. The CLI provides commands for starting, stopping, and visualising pipelines.
- Provides fundamental **signal processing units**: through the extension `ezmsg-sigproc`, users of `ezmsg` have access to over 20 in-built signal processing units, that can be used both in an `ezmsg` context as well as outside of it. See :doc:`sigproc <sigproc>` for more information.
- **Extensible** via extensions: `ezmsg` is designed to be extensible, allowing users to create and share custom components and extensions. This extensibility enables users to tailor the framework to their specific needs and contribute to the broader `ezmsg` community. See :doc:`Extensions <../extensions/content-extensions>` for more information.
- **Open-source**: `ezmsg` is an open-source project, released under the permissive MIT license. This encourages collaboration and contributions from the community, fostering a vibrant ecosystem of users and developers.

In Media 
=========

* `BCI Society 2025 Poster <https://github.com/ezmsg-org/ezmsg/releases/download/V3.6.1/ezmsg_poster_BCI2025.pdf>`_
* `Society For Neuroscience 2024 Poster <https://github.com/ezmsg-org/ezmsg/releases/download/V3.6.1/ezmsg_poster_SfN2024.pdf>`_

Publications
=======================

A collection of academic papers, journals, and other publications that have cited or utilized `ezmsg` in research and development.
These publications provide insights into the practical applications and impact of `ezmsg` in various fields.

- `A click-based electrocorticographic brain-computer interface enables long-term high-performance switch-scan spelling <https://doi.org/10.21203/rs.3.rs-3158792/v1>`_
- `Stable Decoding from a Speech BCI Enables Control for an Individual with ALS without Recalibration for 3 Months <https://doi.org/10.1002/advs.202304853>`_
 

|ezmsg_logo_small| Motivation
********************************

There are many signal processing and message passing frameworks available that employ various approaches and techniques. The original authors of `ezmsg` wanted something akin to (and were inspired by) the directed acyclic graph pub/sub design used by `labgraph <https://github.com/facebookresearch/labgraph>`_. So, `ezmsg` is a pure-Python implementation which is optimized and intended for use in constructing real-time software. `ezmsg` implements much of the `labgraph` API (with a few notable differences), and owes a lot of its design to the `labgraph` developers/project.

On a fundamental level, the `ezmsg` implementation focused on the following major needs:

- lightweight (minimal dependencies), Python-based framework
- modular, directed acyclic graph design of pipelines
- the backend implementation to manage process handling, message passing, and memory management
- `Unit`\ s to require minimal boilerplate code to implement
- easy to use and understand
- open-source and permissively licensed


Differences between `ezmsg` and other frameworks
===========================================================

To be expanded on later.

|ezmsg_logo_small| Extensions
*********************************

`ezmsg` is extensible and already includes several extensions. Here are some of the extensions we manage or are aware of:

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

|ezmsg_logo_small| Financial Support
****************************************

`ezmsg` is supported by Johns Hopkins University (JHU), the JHU Applied Physics Laboratory (APL), and by the Wyss Center for Bio and Neuro Engineering.

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