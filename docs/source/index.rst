
|ezmsg_logo_large|

ezmsg
####################

Welcome to the ezmsg documentation! 

ezmsg is a **highly performant messaging and multiprocessing framework** designed for building modular, high-performance signal processing pipelines. It is built in pure Python with no dependencies, making it easy to install and use.

|ezmsg_logo_small| What is ezmsg?
**********************************

 ``ezmsg`` (n) - "easy message"

 ezmsg is a high-performance execution engine and multiprocessing backend for executing a directed acyclic graph of stream-processing units via a publish-subscribe pattern facilitated by shared memory transfer with zero-copy messaging between composable nodes written in pure Python with no dependencies or stated application space. 

.. todo:: Add diagram of architecture

ezmsg implements much of the LabGraph API (with a few notable differences), and owes a lot of its design to the LabGraph developers/project. Afterall, imitation is the sincerest form of flattery.

The ezmsg library eases the creation of modular systems whose elements can be switched out easily. Consider the use case of building a processing pipeline where you will be experimenting with adding and removing steps until you find an optimal workflow. ezmsg allows you to easily separate each step into a discrete entity and piece together a workflow from those entities. Check out the helpful :doc:`tutorial series <tutorials/content-tutorials>` to see how this works.

.. `Examples <https://github.com/iscoe/ezmsg/tree/master/examples>`_ to see how this works.


|ezmsg_logo_small| Quick start
**************************************

For a detailed description of the installation process and getting started with ezmsg, please refer to the :doc:`Getting Started guide <tutorials/start>`.

But in short, if you have Python 3.10+ installed, you can install ezmsg via pip:

.. code-block:: bash

    pip install ezmsg

Then in your Python script or Jupyter notebook, you can import ezmsg like so:

.. code-block:: python

    import ezmsg.core as ez

If you would like to develop with ezmsg or contribute to the project, you can follow the steps outlined in the :doc:`Developer Guide <developer/content-developer>`.

If you would like to build the documentation locally, please refer to the instructions found in :ref:`documentation_building`. 

A substantial list of extensions exist for ezmsg. One can install extensions as follows:

.. code-block:: bash

    pip install ezmsg[extension_name]


See :doc:`Extensions <../extensions/content-extensions>` for more information.

|ezmsg_logo_small| Documentation Overview
*******************************************

This documentation is designed to help you get started with ezmsg, understand its architecture, and learn how to use it effectively in your projects.

Give it a try with our :doc:`specially created tutorial series <tutorials/content-tutorials>` and see how it can simplify your signal processing needs.

If you don't want to download and install anything just yet, you can check out this `Google Colab page <https://colab.research.google.com/drive/1gHspPyS-lIUpb9zKFzgmBqAZcUPCINSh?usp=sharing>`_ for an example ezmsg notebook to experiment with. It's a little dated, but it should give you a good idea of how to use ezmsg in a Jupyter notebook environment.

These are a few high-level topics to help you learn more about ezmsg and the ezmsg ecosystem.

.. list-table::
   :widths: 50 50
   :class: front_page_table

   * - :doc:`tutorials/content-tutorials`

       Tutorials and examples to help you get started

     - :doc:`tutorials/start`

       Installation instructions for ezmsg

   * - :doc:`explanations/content-explanations`

       About ezmsg, its components and design philosophy

     - :doc:`release`
 
       New features, changes, deprecation notes, and bug fixes

   * - :doc:`reference/content-reference`

       APIs for ezmsg

     - :doc:`how-tos/content-howtos`

       How to use ezmsg in various scenarios and applications

   * - :doc:`Contributor Guides <developer/content-developer>`

       Info for those wanting to contribute and help develop ezmsg

     - :doc:`reference/glossary`

       Glossary of terms used in this documentation


|ezmsg_logo_small| Why use ezmsg?
***********************************

Why use ezmsg over a comparable tool? ezmsg is extremely fast and uses Python's ``multiprocessing.shared_memory`` module to facilitate efficient message passing without C++ or any compilation/build tooling. It is easy to install and contains less boilerplate than similar frameworks. It also provides a framework for building processing pipelines while keeping best practices in mind.


|ezmsg_logo_small| Table of Contents
***************************************

Below is a breakdown of all the documentation available on this site.

.. toctree::
   :maxdepth: 2
   :titlesonly:

   Tutorial <tutorials/content-tutorials>
   What is ezmsg? <explanations/content-explanations>
   How To <how-tos/content-howtos>
   Reference <reference/content-reference>
   Developer <developer/content-developer>
   Extensions <extensions/content-extensions>
   Release Notes <release>


|ezmsg_logo_small| Other Information
**************************************

Financial Support
==========================

`ezmsg` is supported by Johns Hopkins University (JHU), the JHU Applied Physics Laboratory (APL), and by the Wyss Center for Bio and Neuro Engineering.

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


.. |ezmsg_logo_large| image:: _static/_images/ezmsg_logo.png
  :width: 120
  :alt: ezmsg logo

.. |ezmsg_logo_small| image:: _static/_images/ezmsg_logo.png
  :width: 40
  :alt: ezmsg logo
