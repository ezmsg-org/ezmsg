1. Getting Started
###################

System Requirements
*********************

Due to reliance on ``multiprocessing.shared_memory``, ``ezmsg`` requires minimum Python 3.8. Beyond that, ezmsg is a pure Python library with no external dependencies.


**************************
Installing ezmsg
**************************

From PyPi
===============

To install the `ezmsg` framework, you can use pip:

.. code-block:: bash

   pip install ezmsg

Make sure you have Python 3.8 or later installed on your system. You can verify your Python version by running:

.. code-block:: bash

   python --version

Once installed, you can start using `ezmsg` in your projects.

You can of course also clone the repository from GitHub like in the developers section allowing you to alter the base code. See next section for more information.

==========================
For developers
==========================

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


If you want to contribute to the development of `ezmsg`, or write your own pipelinesyou can follow these steps:

1. Fork the repository on GitHub.
2. Create a new branch for your feature or bug fix.
3. Make your changes and commit them with clear messages.
4. Submit a pull request for review.

We welcome contributions and feedback from the community!

uv syncall --all-packages --all-extras

And install the pre-commit hooks:
.. code-block:: bash

   pre-commit install

Testing
^^^^^^^^^^

Testing ezmsg requires:

* pytest
* pytest-cov

We expect a test-driven development, so not only do we expect you to write tests for your code, but also to run the tests before committing your changes. You can do this by running:
.. code-block:: bash

   pytest tests/

**************************
How to update ezmsg
**************************

To update `ezmsg` to the latest version, you can use pip:
.. code-block:: bash

   pip install --upgrade ezmsg

This will ensure you have the latest features and bug fixes. If you have cloned the repository, you can pull the latest changes from the main branch:
.. code-block:: bash

   git pull origin main

*****************************
confirming installation
*****************************

To confirm that `ezmsg` is installed correctly, you can run the following command:

.. code-block:: bash

   pip show ezmsg

This will display information about the installed package, including its version and location.
You can also run a simple test script to check if `ezmsg` is functioning as expected:
.. code-block:: python

   import ezmsg

   print("ezmsg is installed and working correctly!")

****************************
Extensions
****************************

`ezmsg` provides a flexible framework for building and extending messaging pipelines. You can create your own custom message handlers, processors, and pipelines to suit your specific needs.

All of these are optional and can be installed as needed. To install an extension, you can use pip:

.. code-block:: bash

   pip install ezmsg[extension_name]

Here are some ways you can extend `ezmsg`:

1. **Custom Message Handlers**: Create your own message handlers by subclassing the `ezmsg.MessageHandler` class. Implement the `handle_message` method to define how your handler processes incoming messages.

2. **Message Processors**: Build custom message processors by subclassing the `ezmsg.MessageProcessor` class. Processors can be used to modify messages as they pass through the pipeline.

3. **Pipelines**: Combine multiple message handlers and processors into a single pipeline by using the `ezmsg.Pipeline` class. This allows you to create complex processing workflows.

4. **Plugins**: Develop plugins to add new features or integrations to `ezmsg`. Plugins can be distributed as separate packages and easily installed by users.

For more information on creating extensions for `ezmsg`, please refer to the official documentation.