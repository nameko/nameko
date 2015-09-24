.. _installation:

Installation
============

Install with Pip
----------------

You can install nameko and its dependencies from `PyPI <https://pypi.python.org/pypi/nameko>`_ with pip::

    pip install nameko


Source Code
-----------

Nameko is actively developed on `GitHub <https://github.com/onefinestay/nameko>`_. Get the code by cloning the public repository::

    git clone git@github.com:onefinestay/nameko.git

You can install from the source code using setuptools::

    python setup.py install


RabbitMQ
--------

Several of Nameko's built-in features rely on RabbitMQ. Installing RabbitMQ is straightforward on most platforms and they have `excellent documentation <https://www.rabbitmq.com/download.html>`_.

With homebrew on a mac you can install with::

    brew install rabbitmq

On debian-based operating systems::

    apt-get install rabbitmq-server

For other platforms, consult the `RabbitMQ installation guidelines <https://www.rabbitmq.com/download.html>`_.

The RabbitMQ broker will be ready to go as soon as it's installed -- it doesn't need any configuration. The examples in this documentation assume you have a broker running on the default ports on localhost and the `rabbitmq_management <http://www.rabbitmq.com/management.html>`_ plugin is enabled.
