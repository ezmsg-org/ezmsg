# Configuration file for the Sphinx documentation builder.

import os

# -- Project information --------------------------

project = "ezmsg"
copyright = "2022, JHU/APL"
author = "JHU/APL"

# Read version from pyproject.toml
try:
    import tomllib
except ImportError:
    import tomli as tomllib

pyproject_path = os.path.join(os.path.dirname(__file__), "../../pyproject.toml")
with open(pyproject_path, "rb") as f:
    pyproject_data = tomllib.load(f)
    release = pyproject_data["project"]["version"]
    version = release


# -- General configuration --------------------------

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.doctest",
    "sphinx.ext.duration",
    "sphinx.ext.intersphinx",
    "sphinx.ext.linkcode",
    "sphinx.ext.napoleon",
    "sphinx_autodoc_typehints",
    "sphinx_copybutton",
]

templates_path = ["_templates"]

source_suffix = [".rst"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

add_module_names = True

# The toctree master document
master_doc = "index"

# When set to True, `todo` and `todoList` produce output, else they produce nothing.
todo_include_todos = False

# -- Autodoc configuration ------------------------------
autodoc_typehints_format = "short"
python_use_unqualified_type_names = True

# -- Intersphinx configuration --------------------------

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "sphinx": ("https://www.sphinx-doc.org/en/master", None),
    "numpy": ("https://numpy.org/doc/stable", None),
    "ezmsg.sigproc": ("https://www.ezmsg.org/ezmsg-sigproc", None),
    "ezmsg.lsl": ("https://www.ezmsg.org/ezmsg-lsl", None),
    "ezmsg.learn": ("https://www.ezmsg.org/ezmsg-learn", None),
}
intersphinx_disabled_domains = ["std"]

# -- Options for HTML output -----------------------------

html_theme = "pydata_sphinx_theme"
html_logo = "_static/_images/ezmsg_logo.png"
html_favicon = "_static/_images/ezmsg_logo.png"
html_title = f"ezmsg {version}"

html_static_path = ["_static"]

# Timestamp is inserted at every page bottom in this strftime format.
html_last_updated_fmt = "%Y-%m-%d"

# -- Options for EPUB output --------------------------
epub_show_urls = "footnote"

branch = "main"
code_url = f"https://github.com/ezmsg-org/ezmsg/blob/{branch}/"


def linkcode_resolve(domain, info):
    if domain != "py":
        return None
    if not info["module"]:
        return None
    filename = info["module"].replace(".", "/")
    if "core" in filename:
        return f"{code_url}src/ezmsg/core/__init__.py"
    else:
        return f"{code_url}src/{filename}.py"


# -- Options for graphviz -----------------------------
graphviz_output_format = "svg"


def setup(app):
    app.add_css_file("custom.css")
