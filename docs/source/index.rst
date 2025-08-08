
.. image:: logo.png
  :width: 80
  :alt: ezmsg logo

``ezmsg``
#################

Messaging and Multiprocessing.

``ezmsg`` is a pure-python implementation of a directed acyclic graph (DAG) pub/sub messaging pattern based on LabGraph which is optimized and intended for use in constructing real-time neural signal processing implementations. ``ezmsg`` implements much of the LabGraph API (with a few notable differences), and owes a lot of its design to the LabGraph developers/project. Afterall, imitation is the sincerest form of flattery.

The ``ezmsg`` library eases the creation of modular systems whose elements can be switched out easily. Consider the use case of building a processing pipeline where you will be experimenting with adding and removing steps until you find an optimal workflow. ``ezmsg`` allows you to easily separate each step into a discrete entity and piece together a workflow from those entities. Check out the `Examples <https://github.com/iscoe/ezmsg/tree/master/examples>`_ to see how this works.

Why use ``ezmsg`` over a comparable tool? ``ezmsg`` is extremely fast and uses Python's (currently) new multiprocessing.shared_memory module to facilitate efficient message passing without C++ or any compilation/build tooling. It is easy to install and contains less boilerplate than similar frameworks. It also provides a framework for building processing pipelines while keeping best practices in mind.

Check out this `Google Colab <https://colab.research.google.com/drive/1gHspPyS-lIUpb9zKFzgmBqAZcUPCINSh?usp=sharing>`_ for an example ``ezmsg`` notebook to experiment with.

More information
******************

These are a few high-level topics to help you learn more about the Jupyter community and ecosystem.

```{list-table}
:class: front_page_table

* - {doc}`Get started with Jupyter Notebook <start/index>`

      Try the notebook

   - {doc}`Community <community/content-community>`

      Sustainability and growth

* - {doc}`Architecture <projects/architecture/content-architecture>`

      What is Jupyter?

   - {doc}`Contributor Guides <contributing/content-contributor>`

      How to contribute to the projects

* - {doc}`Narratives and Use Cases <use/use-cases/content-user>`

      Narratives of common deployment scenarios

   - {doc}`Release Notes <releases>`

      New features, upgrades, deprecation notes, and bug fixes

* - {doc}`IPython <reference/ipython>`

      An interactive Python kernel and REPL

   - {doc}`Reference <reference/content-reference>`

      APIs

* - {doc}`Installation, Configuration, and Usage <projects/content-projects>`

      Documentation for users

   - {doc}`Advanced <use/advanced/content-advanced>`

      Documentation for advanced use-cases
```

## Table of Contents

The rest of the documentation on this site covers major use-cases of the Jupyter ecosystem, as well as topics that will help you navigate the various parts of the Jupyter community.
For more in-depth documentation about a specific tool, we recommend checking out that tool's documentation (see the list above).

(user-docs)=

```{toctree}
:maxdepth: 2

start/index
```

(jupyter-using)=

```{toctree}
:maxdepth: 2

use/using
```

(jupyter-subprojects)=

```{toctree}
:maxdepth: 2

projects/content-projects
```

```{toctree}
:maxdepth: 2

community/content-community
```

(dev-docs)=

```{toctree}
:maxdepth: 2

contributing/content-contributor
```

```{toctree}
:maxdepth: 2

reference/content-reference
```


.. toctree::
   :maxdepth: 1

   getting-started
   about
   api
   utils
   other
   extensions
   developer
