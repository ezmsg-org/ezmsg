Getting Started
===============

System Requirements
-------------------
Due to reliance on ``multiprocessing.shared_memory``, ``ezmsg`` requires minimum Python 3.8. Beyond that, ezmsg is a pure Python library with no external dependencies.

Testing ezmsg requires:

* pytest
* pytest-cov

Installation
------------

.. TODO: add instructions when it's posted on pypi

Clone the project, then create a virtual environment and install it there.

.. code-block:: powershell

  # Windows
  python3 -m venv env
  env\Scripts\activate
  (env) pip install --upgrade pip
  (env) pip install wheel # Optional, may be useful depending on your platform
  (env) pip install -e ".[test]"

  (env) python -m pytest -v tests # Optionally, Perform tests

.. code-block:: bash

  # Unix-based
  python3 -m venv env
  source env/bin/activate
  (env) pip install --upgrade pip
  (env) pip install wheel # Optional, may be useful depending on your platform
  (env) pip install -e ".[test]"

  (env) python -m pytest -v tests # Optionally, Perform tests

Run an example
--------------

