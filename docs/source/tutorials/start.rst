Installation and Configuration
###############################

This page contains information about installing ezmsg and its extensions. It also provides instructions for developers who want to contribute to the ezmsg project.


|ezmsg_logo_small| System Requirements
***************************************

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


|ezmsg_logo_small| Installing ezmsg
************************************

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

You can also install ezmsg from the source (GitHub respository).  

If you plan to make changes to the code or contribute to the project, this is what you'll need to do. Please see the :doc:`Developer Guide <../developer/content-developer>` for more information on how to develop with ezmsg.

.. note:: This step assumes you have a GitHub account. We generally recommend having a username or email associated with your account that can be tied to you. If you plan on developing for ezmsg, this, along with SSH keys and 2FA will be strictly necessary. 

.. image:: ../_static/_images/cloning.png
   :width: 200
   :align: center
   :alt: GitHub Clone URL image

First clone the project from GitHub. This is done by visiting the `ezmsg repository <https://github.com/ezmsg-org/ezmsg#>`_. There is a green button called "Code" which, when clicked, will show you the URL to clone the repository. If you simply plan to build from source, you can choose the HTTPS URL. 



If you plan on contributing to the project, follow the instructions in the :doc:`Developer section <../developer/install>` to first fork the repository to your own account and then clone using the SSH URL. You will then be required to set up SSH keys as explained in `this link <https://docs.github.com/en/authentication/connecting-to-github-with-ssh/adding-a-new-ssh-key-to-your-github-account>`_.

Once you have the URL, you can clone the respository using the following command:

.. code-block:: bash

   cd <path-to-ezmsg-parent-directory>
   git clone <repository-url>

.. note:: the ``path-to-ezmsg-parent-directory`` is the directory where you want to clone the ezmsg repository. Once you clone the repository there will be a new directory called ``ezmsg`` in that location. So, don't create a new directory called ``ezmsg`` in that location, just clone it directly into the parent directory.

Next create a virtual environment and install it there.

.. code-block:: powershell

  # Windows
  python3 -m venv env
  env\Scripts\activate
  (env) pip install --upgrade pip
  (env) pip install wheel # Optional, may be useful depending on your platform
  (env) pip install -e ".[test]"

.. code-block:: bash

  # Unix-based
  python3 -m venv env
  source env/bin/activate
  (env) pip install --upgrade pip
  (env) pip install wheel # Optional, may be useful depending on your platform
  (env) pip install -e ".[test]"

Finally, you can optionally run the repository tests to ensure everything is working correctly. 

.. code-block:: bash

  (env) python -m pytest -v tests # Optionally, perform tests


|ezmsg_logo_small| How to update ezmsg
***************************************

Updating ezmsg ensures you have the latest features, improvements, and bug fixes. 

If you installed ezmsg using pip, you can easily update it using the following command:

.. code-block:: bash

  pip install --upgrade ezmsg

If you installed ezmsg by cloning the ezmsg repository, you can pull the latest changes from the main branch:

.. code-block:: bash

   git pull origin main


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

with the following code to check if ezmsg is functioning as expected:

.. code-block:: bash

   python3 test_ezmsg.py


|ezmsg_logo_small| Installing Extensions
*****************************************

ezmsg comes with a whole host of extensions that can be installed to add extra functionality. All of these are optional and can be installed as needed. 

To install an extension, you can use pip:

.. code-block:: bash

   pip install ezmsg[extension_name]

For more information on available extensions, please refer to the :doc:`Extensions page <../extensions>`.


|ezmsg_logo_small| Ready to build your first ezmsg pipeline?
**************************************************************

You are now ready to start building your first ezmsg pipeline! Click Next below or head to :doc:`pipeline` to get started.

.. |ezmsg_logo_small| image:: ../_static/_images/ezmsg_logo.png
  :width: 40
  :alt: ezmsg logo
