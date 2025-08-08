ezmsg Documentation
#######################

Anyone planning to develop or contribute to `ezmsg`, should adhere to good documentation practices. 

|ezmsg_logo_small| Docstrings
=================================

All functions and classes should be documented with docstrings. The docstrings should be clear, concise, and easy to read. 

A typical docstring will contain:

- a short summary (usually a single sentence) of the function or class
- a paragraph of additional notes, which might include notes on usage, or anything else that 
- a list of function parameters or class attributes (with type annotation)
- the return value and any exceptions raised by a function should be listed and described.
- (optionally) code snippets demonstrating usage (only for major public API functions or classes)

There are many styles for writing docstrings. In `ezmsg`, we follow the `PEP 287 <https://peps.python.org/pep-0287/>`_ standard.  The documentation package (Sphinx) can also read docstrings written in the `Google Python Style Guide <https://google.github.io/styleguide/pyguide.html>`_.


.. _documentation_building:

|ezmsg_logo_small| Building ezmsg Documentation
================================================

The documentation is built using Sphinx. Thus, our documentation content must be compatible with Sphinx tools.

To build the documentation locally into HTML format, you need to have the development environment set up as described in the `developer install documentation <install>`_. Then

- change directory to the `docs` folder in the ezmsg repository:

.. code-block:: bash

    $ cd path/to/your/ezmsg/docs

- Build the documentation:

.. code-block:: bash

    # on Windows
    $ uv run .\make.bat html

    # on Linux, MacOS
    $ uv run make html


|ezmsg_logo_small| Documentation Standards
============================================

The documentation is written in `reStructuredText (ReST) <https://docutils.sourceforge.io/rst.html>`_. `Here <https://bashtage.github.io/sphinx-material/rst-cheatsheet/rst-cheatsheet.html>`_ is a convenient ReST cheat sheet.

The documentation contains four main sections representing our approach to documentation:

1. **Tutorial**: step-by-step guides for new users to get started with ezmsg. Not intended to be comprehensive, but rather to get users up and running quickly.
2. **Explanation**: A more in-depth explanation of the concepts and architecture of ezmsg. This section is intended for users who want to understand how ezmsg works under the hood.
3. **How-To**: Practical guides on how to accomplish specific tasks using ezmsg. This section is intended for users who want to learn how to use ezmsg for specific use cases.
4. **Reference API**: Comprehensive reference documentation for all public classes, functions, and modules in ezmsg. This section is intended for users who need detailed information about the ezmsg API.

The API is intended to be self-updating using `sphinx-autodoc <https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html>`_. Thus, all public classes and functions should be documented with docstrings as described in the previous section.

If a feature is worth describing in further detail, please consider adding it to the documentation in the appropriate section.

Consider also adding code examples and usage snippets where appropriate. This can greatly enhance the usability of the documentation. Example scripts can also be added to the `examples` folder in the root of the ezmsg repository.



.. |ezmsg_logo_small| image:: ../_static/_images/ezmsg_logo.png
  :width: 40
  :alt: ezmsg logo
