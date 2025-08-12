Installation and Configuration
###############################

This page contains information about installing ezmsg and its extensions. It also provides instructions for developers who want to contribute to the ezmsg project.


System Requirements
*********************

ezmsg is written in and for **Python**, so it requires a Python interpreter to run. Due to reliance on the in-built ``multiprocessing.shared_memory`` module, ezmsg requires at minimum Python 3.8. Beyond that, ezmsg is a pure Python library with no external dependencies.

This also means that ezmsg is cross-platform and should run on any operating system that supports Python, including Windows, macOS, and Linux.

In Windows and macOS, Python can be downloaded from the official website: `python.org <https://www.python.org/downloads/>`_. For newer users, we recommend using the `Anaconda <https://www.anaconda.com/download>`_ distribution of Python, which comes with a package manager and many useful libraries pre-installed.

On Linux, Python is usually pre-installed, but you can also install it using your package manager: 

.. code-block:: bash

   # For Debian/Ubuntu-based systems
   sudo apt install python3.<version> python3-pip -y

   # For Red Hat/CentOS-based systems
   sudo dnf install python3.<version> python3-pip -y

   # For Arch Linux-based systems
   sudo pacman -S python3.<version> python-pipx

where ``3.<version>`` is the version of Python you want to install (e.g. 3.12).

Whichever you choose, ensure that you have Python 3.8 or later installed on your system. You can verify your Python version by running:

.. code-block:: bash

   python3 --version


Installing ezmsg
**************************

We can install ezmsg using :term:`pip`, which is the package manager for Python. It will handle all dependencies and ensure that you have the latest version. It will download the package from the Python Package Index (:term:`PyPI`) and install it on your system.

You can alternatively run ezmsg from source by cloning the repository, but this is only recommended for those planning to do some ezmsg development or if you have specific customization needs.

From PyPI (using pip)
======================

To install the ezmsg framework, you can use pip:

.. code-block:: bash

   pip install ezmsg

Once installed, you can start using ezmsg in your projects.

From source (using git)
==========================

You can also install ezmsg from the source (GitHub respository).  Please see the :doc:`Developer Guide <../developer/content-developer>` for more information on how to do this.

Clone the project, then create a virtual environment and install it there.

.. code-block:: powershell

  # Windows
  python3 -m venv env
  env\Scripts\activate
  (env) pip install --upgrade pip
  (env) pip install wheel # Optional, may be useful depending on your platform
  (env) pip install -e ".[test]"

  (env) python -m pytest -v tests # Optionally, perform tests

.. code-block:: bash

  # Unix-based
  python3 -m venv env
  source env/bin/activate
  (env) pip install --upgrade pip
  (env) pip install wheel # Optional, may be useful depending on your platform
  (env) pip install -e ".[test]"

  (env) python -m pytest -v tests # Optionally, perform tests



How to update ezmsg
**************************

Updating ezmsg ensures you have the latest features, improvements, and bug fixes. 

If you installed ezmsg using pip, you can easily update it using the following command:

.. code-block:: bash

  pip install --upgrade ezmsg

If you installed ezmsg by cloning the ezmsg repository, you can pull the latest changes from the main branch:

.. code-block:: bash

   git pull origin main


Confirming installation
*****************************

To confirm that ezmsg is installed correctly, you can run the following command:

.. code-block:: bash

   pip show ezmsg

This will display information about the installed package, including its version and location.
You can also run a simple test script to check if ezmsg is functioning as expected:

.. code-block:: python

   import ezmsg

   print("ezmsg is installed and working correctly!")


Extensions
****************************

ezmsg comes with a whole host of extensions that can be installed to add extra functionality. All of these are optional and can be installed as needed. 

To install an extension, you can use pip:

.. code-block:: bash

   pip install ezmsg[extension_name]

For more information on available extensions, please refer to the :doc:`Extensions page <../extensions>`.


Ready to build your first ezmsg pipeline?
***********************************************

You are now ready to start building your first ezmsg pipeline! Click next below or head to :doc:`pipeline` to get started.