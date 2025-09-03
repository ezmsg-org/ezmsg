ezmsg Documentation
#######################

(under construction)

This document is intended for developers who are maintaining the Blackrock BCI Documentation. It is a guide to the documentation practices, including docstrings, READMEs, tutorials, and publishing the documentation.

Content
#######################

The documentation should be clear, concise, and easy to read. The documentation should be organized into sections, with headings and subheadings to help the reader navigate the document.

Tools
#######################

The documentation is built using Sphinx. Thus, our documentation content must be compatible with Sphinx tools.

* We use the [MyST parser](https://myst-parser.readthedocs.io/en/latest/) to support Markdown documents in Sphinx.
* We use [MyST-NB](https://myst-nb.readthedocs.io/en/latest/) to generate documentation from [mixed code and text](https://en.wikipedia.org/wiki/Literate_programming).
* We use [sphinx.ext.napoleon](https://www.sphinx-doc.org/en/master/usage/extensions/napoleon.html) to parse Google-style docstrings.

Docstrings
#######################

Docstrings are written according to the [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html).

Configure your IDE to use the Google Python Style Guide for docstrings.

* PyCharm: `Settings` > `Tools` > `Python Integrated Tools` > `Docstrings` > `Docstring format` > `Google`
    * This must be repeated for each namespace module.
* VSCode: Install the [Google Docstring Generator](https://marketplace.visualstudio.com/items?itemName=njpwerner.autodocstring) extension.

Mixed Code and Markdown
#################################

You can write documentation using Jupyter notebooks with Markdown and runnable code cells. This is useful for tutorials and examples.

Alternatively, you can include code cells in your Markdown files using the following header:

.. ````
.. ---
.. file_format: mystnb
.. kernelspec:
..   name: python3
.. ---
.. ````

.. The following example indicates how to include a code cell:

.. ````
.. ```{code-cell}
.. print("Hello, World!")
.. ```
.. ````

Build
###########

You must configure your environment as described in the [developer pre-requisites documentation](Developers.md#pre-requisites).

Then, activate the virtual environment and navigate to the `docs` folder:

.. ```bash
.. source .venv/bin/activate
.. cd docs
.. ```

Then 

`sphinx-build -nW --keep-going -b html ./ _build/html`

or simply

`make html`

(clear the venv session with `deactivate`)