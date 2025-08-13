Install instructions for developers
######################################################

(under construction)

### Pre-requisites

1. [`uv`](https://docs.astral.sh/uv/getting-started/installation/)
2. [ssh keys to access GitHub](https://docs.github.com/en/authentication/connecting-to-github-with-ssh)

### Setup

1. Clone this repo.
2. `cd brnbci`
3. `uv sync --all-packages --all-extras` to install all dependencies.
4. `uv run pre-commit install` to install pre-commit hooks.



You can also install ezmsg from the source (GitHub respository).  like in the developers section allowing you to alter the base code. See next section for more information.
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

   uv run pre-commit install

Testing
^^^^^^^^^^

Testing ezmsg requires:

* pytest
* pytest-cov

We expect a test-driven development, so not only do we expect you to write tests for your code, but also to run the tests before committing your changes. You can do this by running:
.. code-block:: bash

   pytest tests/