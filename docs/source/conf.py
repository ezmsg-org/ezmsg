# Configuration file for the Sphinx documentation builder.

# -- Project information --------------------------

project = "ezmsg"
copyright = "2022, JHU/APL"
author = "JHU/APL"

release = "3.3.4"
version = "3.3.4"

# -- General configuration --------------------------

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.doctest",
    "sphinx.ext.duration",
    "sphinx.ext.graphviz",
    "sphinx.ext.intersphinx",
    "sphinx.ext.linkcode",
    "sphinx.ext.napoleon",
    "sphinxext.rediraffe",
    "myst_parser",
    "sphinx_autodoc_typehints",
    "sphinx_copybutton",
    "sphinx_design",
]

templates_path = ["_templates"]

source_suffix = [".rst", ".md"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# The toctree master document
master_doc = "index"

# When set to True, `todo` and `todoList` produce output, else they produce nothing.
todo_include_todos = False

# -- Autodoc configuration ------------------------------
autodoc_typehints_format = "short"
python_use_unqualified_type_names = True
autodoc_type_aliases = {
    "numpy.typing.NDArray": "numpy NDArray",
}

# -- Intersphinx configuration --------------------------

intersphinx_mapping = {
    "python": ("https://docs.python.org/3/", None),
    "sphinx": ("https://www.sphinx-doc.org/en/master/", None),
    "numpy": ("https://numpy.org/doc/stable/", None),
    "scipy": ("https://docs.scipy.org/doc/scipy/", None),
}
intersphinx_disabled_domains = ["std"]

# -- Options for HTML output -----------------------------

html_theme = "pydata_sphinx_theme"
html_logo = "_static/_images/ezmsg_logo.png"
html_favicon = "_static/_images/ezmsg_logo.png"

html_static_path = ["_static"]

# Redirects for pages that are unavailable or moved
rediraffe_redirects = {
    "about.rst": "explanations/content-explanations.rst",
    "getting-started.rst": "tutorials/start.rst",
}

# Timestamp is inserted at every page bottom in this strftime format.
html_last_updated_fmt = '%Y-%m-%d'

# -- Options for EPUB output --------------------------
epub_show_urls = "footnote"

add_module_names = False

branch = "main"
code_url = f"https://github.com/iscoe/ezmsg/blob/{branch}/"
sigproc_code_url = f"https://github.com/ezmsg-org/ezmsg-sigproc/blob/{branch}/"


def linkcode_resolve(domain, info):
    if domain != "py":
        return None
    if not info["module"]:
        return None
    filename = info["module"].replace(".", "/")
    if "sigproc" in filename:
        return f"{sigproc_code_url}src/{filename}.py"
    elif "core" in filename:
        return f"{code_url}src/ezmsg/core/__init__.py"
    else:
        return f"{code_url}src/{filename}.py"

# -- Options for graphviz -----------------------------
graphviz_output_format = "svg"

def setup(app):
    app.add_css_file("custom.css")