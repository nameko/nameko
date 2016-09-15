Contributing
============

Nameko is developed on `GitHub <https://github.com/onefinestay/nameko>`_ and contributions are welcome.

Use GitHub `issues <https://github.com/onefinestay/nameko/issues>`_ to report bugs and make feature requests.

You're welcome to `fork <https://github.com/onefinestay/nameko/fork>`_ the repository and raise a pull request with your contributions.

You can install all the development dependencies using::

    pip install -e .[dev]

and the requirements for building docs using::

    pip install -e .[docs]


Pull requests are automatically built with `Travis-CI <https://travis-ci.org/onefinestay/nameko/>`_. Travis will fail the build unless all of the following are true:

    * All tests pass
    * 100% line coverage by tests
    * Documentation builds successfully (including spell-check)

See :ref:`getting in touch <getting_in_touch>` for more guidance on contributing.

Running the tests
--------------------

There is a Makefile with convenience commands for running the tests. To run them locally you must have RabbitMQ :ref:`installed <installation>` and running, then call::

    $ make test
