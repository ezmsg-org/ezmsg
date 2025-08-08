Developer Conventions and Standards
########################################

When intending to contribute to the ezmsg codebase, please follow the conventions and standards outlined in this section.

|ezmsg_logo_small| Types of Contributions
==========================================

There are many ways to contribute to the ezmsg project. These include:

- Bug fixes
- New features
- Changes to existing features
- Documentation improvements

For each of these, please open an issue on the main `ezmsg GitHub repository <https://github.com/ezmsg-org/ezmsg/issues>`_.

Generally, we suggest structuring the issue as follows:

- **A clear and descriptive title**
- **A detailed description of the issue or feature request**. We find that describing the use case in detail is more helpful than prescribing a specific implementation. It allows the wider ezmsg community to provide input on the best way to implement the feature.
- **Context**: This can be
    - **Steps to reproduce the issue** (for bug reports)
    - **Examples of how the feature would be used** (for feature requests)
    - **Screenshots or code snippets** (if applicable)


|ezmsg_logo_small| Pushing Code to ezmsg repository
====================================================

Understanding how GitHub works is essential for contributing code to the ezmsg repository. We use `git-flow <https://nvie.com/posts/a-successful-git-branching-model/>`_ for branching (the link is for educational purposes - where it differs from the below, please follow this documentation instead). The ``main`` branch is the release branch of `ezmsg`. All development should be done on top of the development ``dev`` branch. 

First, ensure the ``dev`` branch in your forked repository is up-to-date with the ``dev`` branch of the main repository. You can do this by synchronising your forked ``dev`` branch on the GitHub website or from the terminal by setting up a remote that points to the main repository and pulling the latest changes:

.. code-block:: bash
    
    git remote add upstream https://github.com/ezmsg-org/ezmsg.git
    git checkout dev
    git pull upstream dev

Rather than working directly on the ``dev`` branch, please create a branch off of ``dev`` for your changes. This keeps your work isolated and makes it easier to manage multiple contributions. Your new branch should indicate if it is a bugfix, feature, or other type of change. You can create a new branch using:

.. code-block:: bash
    
    git checkout -b feature/your-feature-name
    # or for bug fixes
    git checkout -b bugfix/your-bugfix-name
    # or for documentation changes
    git checkout -b docs/your-docs-change-name

After making your changes, commit them with clear and descriptive messages. Commit messages should follow the conventional commit style by answering the question "What will this commit do?". For example:

.. code-block:: bash

   git add .
   git commit -m "Add <new feature> to ezmsg"

Then push your branch to your forked repository:

.. code-block:: bash

   git push origin feature/your-feature-name

Finally, when you are ready to submit your changes, create a `Pull Request <https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork>`_ from your branch to the ``dev`` branch of the main ezmsg repository. Please ensure that your pull request includes a clear description of the changes made and any relevant context. A good pull request has the following details:

- **Description**: A concise summary of the changes. Preferably, this should reference the issue it addresses.
- **Type of Change**: Whether it is a bug fix, new feature, documentation update, etc.
- **Changes affecting current usage**: Explain if the changes will affect existing users of ezmsg, and how.
- **Changes**: A list of the main changes made in the codebase.
- **Testing**: Describe how the changes were tested, including any new tests added.


|ezmsg_logo_small| Code Style
==============================

We follow standard Python code standards based on ` PEP 8 <https://peps.python.org/pep-0008/>`_. We strongly suggest using a formatter and linter in order to ensure following standard Python code conventions. We use the `ruff <https://docs.astral.sh/ruff/>`_ package that comes with `uv`. 

One should double check their code before committing for formatting and linting issues. with `ruff` you can run:

- ``uv run ruff format .`` for formatting checks.
- ``uv run ruff check .``` for linting checks. 


|ezmsg_logo_small| Documentation Standards
==============================================

All functions and classes should be documented with docstrings. If it is a public API function or class, the docstring should be comprehensive and follow the recommended docstring style. One should also consider adding any useful information to the `ezmsg` documentation in the `docs` folder of the repository.

Docstrings in ezmsg follow `PEP 287 <https://peps.python.org/pep-0287/>`_. The documentation package (sphinx) can also read docstrings written in the `Google Python Style Guide <https://google.github.io/styleguide/pyguide.html>`_.

For more information on writing documentation for ezmsg, please see the `developer documentation guide <documentation>`_.

|ezmsg_logo_small| Testing
=============================

Writing code for `ezmsg` should always be accompanied by tests. Please follow the Test-Driven Development (TDD) approach where possible. This means writing tests before implementing the actual functionality. All existing and new tests should be run and pass before any code is committed.

You can run tests using `pytest` using the following command:

.. code-block:: bash

   uv run pytest tests/

.. |ezmsg_logo_small| image:: ../_static/_images/ezmsg_logo.png
  :width: 40
  :alt: ezmsg logo
