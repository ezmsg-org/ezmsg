Developer Conventions
########################

(under construction)

Code Style
===========

We use the [Ruff](https://docs.astral.sh/ruff/) package for formatting and code style. The pre-commit hooks will run the ruff linter and formatter, but it's better form to check this yourself first:
- ``uv run ruff format .`` for formatting checks.
- ``uv run ruff check .``` for linting checks. 

Docstrings are to be written according to the [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html).

Contributing
==============

We use "git flow" for branching. Please create a feature branch off of the ``dev`` branch. When you are ready to merge, please create a pull request to merge your feature branch into the ``dev`` branch. Once the feature is complete, we will merge the ``dev`` branch into the ``main`` branch then create a new release. As of this writing, brnbci is not published anywhere so the release is just a tag in the repository.