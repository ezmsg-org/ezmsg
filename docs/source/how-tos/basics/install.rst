How to install ezmsg?
###########################


You can install ezmsg in two main ways: using :term:`pip` to install from the Python Package Index (:term:`PyPI`) or by cloning the source code from the GitHub repository. The first method is described below and the second method is described in :doc:`../../developer/install`. Use the first method if you are using ezmsg and the latter if you plan to develop for ezmsg.

|ezmsg_logo_small| System Requirements
***************************************

ezmsg is written in and for **Python**, so it requires a Python interpreter to run. Currently, ezmsg requires at minimum Python 3.10. Beyond that, ezmsg is a pure Python library with no external dependencies.

This also means that ezmsg is cross-platform and should run on any operating system that supports Python, including Windows, macOS, and Linux.

In Windows and macOS, Python can be downloaded from the official website: `python.org <https://www.python.org/downloads/>`_. For newer users, we recommend using the `Anaconda <https://www.anaconda.com/download>`_ distribution of Python, which comes with a package manager and many useful libraries pre-installed.

On Linux, Python is usually pre-installed, but you can also install it using your package manager in your :term:`terminal`: 

.. code-block:: bash

   # For Debian/Ubuntu-based systems
   sudo apt install python3.<version> python3-pip -y

   # For Red Hat/CentOS-based systems
   sudo dnf install python3.<version> python3-pip -y

   # For Arch Linux-based systems
   sudo pacman -S python3.<version> python-pipx

where ``3.<version>`` is the version of Python you want to install (e.g. 3.12). 

Whichever you choose, ensure that you have Python 3.8 or later installed on your system. You can verify your Python version by running in your terminal:

.. code-block:: bash

   python3 --version


|ezmsg_logo_small| Installing ezmsg
************************************

We can install ezmsg using :term:`pip`, which is the package manager for Python. It will handle all dependencies and ensure that you have the latest version. It will download the package from the Python Package Index (:term:`PyPI`) and install it on your system. To install the ezmsg framework, run the following code in your terminal:

.. code-block:: bash

   pip install ezmsg



.. _updating-ezmsg:

|ezmsg_logo_small| How to update ezmsg
***************************************

Updating ezmsg ensures you have the latest features, improvements, and bug fixes. 

If you installed ezmsg using pip, you can easily update it using the following command in your terminal:

.. code-block:: bash

  pip install --upgrade ezmsg


|ezmsg_logo_small| Confirming installation
*******************************************

To confirm that ezmsg is installed correctly, you can run the following command:

.. code-block:: bash

   pip show ezmsg

This will display information about the installed package, including its version and location.
You can also run a simple test script: 

.. code-block:: python

   # test_ezmsg.py
   import ezmsg

   print("ezmsg is installed and working correctly!")

with the following code (in the terminal) to check if ezmsg is functioning as expected:

.. code-block:: bash

   python3 test_ezmsg.py


|ezmsg_logo_small| Installing Extensions
*****************************************

ezmsg comes with a whole host of extensions that can be installed to add extra functionality. All of these are optional and can be installed as needed. 

To install an extension, you can use pip:

.. code-block:: bash

   pip install ezmsg[extension_name]

For more information on available extensions, please refer to the :doc:`Extensions page <../extensions/content-extensions>`.


.. |ezmsg_logo_small| image:: ../../_static/_images/ezmsg_logo.png
  :width: 40
  :alt: ezmsg logo
