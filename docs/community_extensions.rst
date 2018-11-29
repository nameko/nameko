.. _community_extensions:

Community
=========

There are a number of nameko extensions and supplementary libraries that are not part of the core project but that you may find useful when developing your own nameko services:

Extensions
----------

    * `nameko-sqlalchemy <https://github.com/onefinestay/nameko-sqlalchemy>`_

        A ``DependencyProvider`` for writing to databases with SQLAlchemy. Requires a pure-python or otherwise eventlet-compatible database driver.

        Consider combining it with `SQLAlchemy-filters <https://github.com/Overseas-Student-Living/sqlalchemy-filters>`_ to add filtering, sorting and pagination of query objects when exposing them over a REST API.

    * `nameko-sentry <https://github.com/mattbennett/nameko-sentry>`_

        Captures entrypoint exceptions and sends tracebacks to a `Sentry <https://getsentry.com/>`_ server.

    * `nameko-amqp-retry <https://github.com/nameko/nameko-amqp-retry>`_

        Nameko extension allowing AMQP entrypoints to retry later.

    * `nameko-bayeux-client <https://github.com/Overseas-Student-Living/nameko-bayeux-client>`_

        Nameko extension with a Cometd client implementing Bayeux protocol

    * `nameko-slack <https://github.com/iky/nameko-slack>`_

        Nameko extension for interaction with Slack APIs. Uses Slack Developer Kit for Python.

    * `nameko-eventlog-dispatcher <https://github.com/sohonetlabs/nameko-eventlog-dispatcher>`_

        Nameko dependency provider that dispatches log data using Events (Pub-Sub).

    * `nameko-redis-py <https://github.com/fraglab/nameko-redis-py>`_

        Redis dependency and utils for Nameko.

    * `nameko-redis <https://github.com/etataurov/nameko-redis/>`_

        Redis dependency for nameko services

    * `nameko-statsd <https://github.com/sohonetlabs/nameko-statsd>`_

        A StatsD dependency for nameko, enabling services to send stats.

    * `nameko-twilio <https://github.com/invictuscapital/nameko-twilio>`_

        Twilio dependency for nameko, so you can send SMS, make calls, and answer calls in your service.

    * `nameko-sendgrid <https://github.com/invictuscapital/nameko-sendgrid>`_

        SendGrid dependency for nameko, for sending transactional and marketing emails.

    * `nameko-cachetools <https://github.com/santiycr/nameko-cachetools>`_

        Tools to cache RPC interactions between your nameko services.

Supplementary Libraries
-----------------------

    * `django-nameko <https://github.com/and3rson/django-nameko>`_

        Django wrapper for Nameko microservice framework.

    * `flask_nameko <https://github.com/clef/flask-nameko>`_

        A wrapper for using nameko services with Flask.

    * `nameko-proxy <https://github.com/fraglab/nameko-proxy>`_

        Standalone async client to communicate with Nameko microservices.

Search PyPi for more `nameko packages <https://pypi.python.org/pypi?%3Aaction=search&term=nameko&submit=search>`_

If you would like your own nameko extension or library to appear on this page, please :ref:`get in touch <getting_in_touch>`.
