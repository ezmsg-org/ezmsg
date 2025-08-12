
|ezmsg_logo_large|

ezmsg
####################

Welcome to the ezmsg documentation! 

``ezmsg`` is a **highly performant messaging and multiprocessing framework** designed for building modular, high-performance signal processing pipelines. It is built in pure Python with no dependencies, making it easy to install and use.

This documentation is designed to help you `get started <tutorials/start>`_ with ``ezmsg``, understand its architecture, and learn how to use it effectively in your projects.

What is ezmsg?
*****************

 ``ezmsg`` (n) - "easy message"

 ``ezmsg`` is a high-performance execution engine and multiprocessing backend for executing a directed acyclic graph of stream-processing units via a publish-subscribe pattern facilitated by shared memory transfer with zero-copy messaging between composable nodes written in pure Python with no dependencies or stated application space. 

``ezmsg`` implements much of the LabGraph API (with a few notable differences), and owes a lot of its design to the LabGraph developers/project. Afterall, imitation is the sincerest form of flattery.

The ``ezmsg`` library eases the creation of modular systems whose elements can be switched out easily. Consider the use case of building a processing pipeline where you will be experimenting with adding and removing steps until you find an optimal workflow. ``ezmsg`` allows you to easily separate each step into a discrete entity and piece together a workflow from those entities. Check out the helpful  `Tutorial <tutorials/content-tutorials>`_ to see how this works.

.. `Examples <https://github.com/iscoe/ezmsg/tree/master/examples>`_ to see how this works.


Where do I start?
***********************

Give it a try with our `specially created tutorial <tutorials/content-tutorials>`_ and see how it can simplify your signal processing needs.

If you don't want to download and install anything just yet, you can check out this `Google Colab page <https://colab.research.google.com/drive/1gHspPyS-lIUpb9zKFzgmBqAZcUPCINSh?usp=sharing>`_ for an example ``ezmsg`` notebook to experiment with. It's a little dated, but it should give you a good idea of how to use ``ezmsg`` in a Jupyter notebook environment.


Why use ezmsg?
***********************

Why use ``ezmsg`` over a comparable tool? ``ezmsg`` is extremely fast and uses Python's ``multiprocessing.shared_memory`` module to facilitate efficient message passing without C++ or any compilation/build tooling. It is easy to install and contains less boilerplate than similar frameworks. It also provides a framework for building processing pipelines while keeping best practices in mind.


More information
******************

These are a few high-level topics to help you learn more about ``ezmsg`` and the ``ezmsg`` ecosystem.

.. list-table::
   :widths: 50 50
   :class: front_page_table

   * - :doc:`tutorials/content-tutorials`

       Getting started! Tutorials and examples to help you get started with ``ezmsg``.

     - :doc:`tutorials/start`

       Installation instructions for ``ezmsg``.

   * - :doc:`explanations/content-explanations`

       About ``ezmsg``, its components and design philosophy.

     - :doc:`release`
 
       Release notes, new features, upgrades, deprecation notes, and bug fixes.

   * - :doc:`reference/content-reference`

       API for ``ezmsg``.

     - :doc:`how-tos/content-howtos`

       How to use ``ezmsg`` in various scenarios and applications.

   * - :doc:`developer/content-developer`

       Information for those wanting to contribute and help develop ezmsg.

     - :doc:`reference/glossary`

       Glossary of terms used in this documentation.


|ezmsg_logo_small| Table of Contents
***************************************

The rest of the documentation on this site covers major use-cases of the Jupyter ecosystem, as well as topics that will help you navigate the various parts of the Jupyter community.
For more in-depth documentation about a specific tool, we recommend checking out that tool's documentation (see the list above).



.. toctree::
   :maxdepth: 2

   Tutorial <tutorials/content-tutorials>
   What is ezmsg? <explanations/content-explanations>
   How To <how-tos/content-howtos>
   Reference <reference/content-reference>
   Developer <developer/content-developer>
   Extensions <extensions>
   Release Notes <release>

.. |ezmsg_logo_large| image:: _static/_images/ezmsg_logo.png
  :width: 120
  :alt: ezmsg logo

.. |ezmsg_logo_small| image:: _static/_images/ezmsg_logo.png
  :width: 40
  :alt: ezmsg logo
