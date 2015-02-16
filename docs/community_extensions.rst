.. _community_extensions:

Community Extensions
--------------------

There are a number of extensions that are not part of the core library but that you may find useful when developing your own nameko services:

    * `nameko-sqlalchemy <http://pypi.python.org/pypi/nameko-sqlalchemy>`_

        A ``DependencyProvider`` for writing to databases with SQLAlchemy. Requires a pure-python or otherwise eventlet-compatible database driver.

    * nameko-memcached (coming soon)

        A ``DependencyProvider`` for accessing a `Memcached <http://memcached.org/>`_ cluster.

    * nameko-sentry (coming soon)

        Captures entrypoint exceptions and sends tracebacks to a `Sentry <https://getsentry.com/>`_ server.

    * nameko-logstash (coming soon)

        Sends entrypoint execution details to a `Logstash <http://logstash.net/>`_ server.


If you would like your own extension to appear in this list, please :ref:`get in touch <getting_in_touch>`.
