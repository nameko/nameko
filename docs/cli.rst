Command Line Interface
======================

Nameko ships with a command line interface to make hosting and interacting with services as easy as possible.

.. _running_a_service:

Running a Service
-----------------

.. code-block:: shell

    $ nameko run <module>[:<ServiceClass>]

Discover and run a service class. This will start the service in the foreground and run it until the process terminates.

It is possible to override the default settings using a ``--config`` switch

.. code-block:: shell

    $ nameko run --config ./foobar.yaml <module>[:<ServiceClass>]


and providing a simple YAML configuration file:

.. code-block:: yaml

    # foobar.yaml

    AMQP_URI: 'pyamqp://guest:guest@localhost'
    WEB_SERVER_ADDRESS: '0.0.0.0:8000'
    rpc_exchange: 'nameko-rpc'
    max_workers: 10
    parent_calls_tracked: 10

    LOGGING:
        version: 1
        handlers:
            console:
                class: logging.StreamHandler
        root:
            level: DEBUG
            handlers: [console]


The ``LOGGING`` entry is passed to :func:`logging.config.dictConfig` and should conform to the schema for that call.

Config values can be read via the built-in :ref:`config_dependency_provider` dependency provider.


Environment variable substitution
---------------------------------
YAML configuration files have support for environment variables.
You can use bash style syntax: ``${ENV_VAR}``.
Optionally you can provide default values ``${ENV_VAR:default_value}``.
Default values can contains environment variables recursively ``${ENV_VAR:default_${OTHER_ENV_VAR:value}}`` (note: this feature requires regex package).


.. code-block:: yaml

    # foobar.yaml
    AMQP_URI: pyamqp://${RABBITMQ_USER:guest}:${RABBITMQ_PASSWORD:password}@${RABBITMQ_HOST:localhost}

To run your service and set environment variables for it to use:

.. code-block:: shell

    $ RABBITMQ_USER=user RABBITMQ_PASSWORD=password RABBITMQ_HOST=host nameko run --config ./foobar.yaml <module>[:<ServiceClass>]

If you need to quote the values in your YAML file, the explicit ``!env_var`` resolver is required:

.. code-block:: yaml

    # foobar.yaml
    AMQP_URI: !env_var "pyamqp://${RABBITMQ_USER:guest}:${RABBITMQ_PASSWORD:password}@${RABBITMQ_HOST:localhost}"

If you need to use values as raw strings in your YAML file without them getting converted to native python,
the explicit ``!raw_env_var`` resolver is required:

.. code-block:: yaml

    # foobar.yaml
    ENV_THAT_IS_NEEDED_RAW: !raw_env_var "${ENV_THAT_IS_NEEDED_RAW:1234.5660}"

This will turn into the string value ``1234.5660``, instead of a float number.

You can provide many levels of default values

.. code-block:: yaml

    # foobar.yaml
    AMQP_URI: ${AMQP_URI:pyamqp://${RABBITMQ_USER:guest}:${RABBITMQ_PASSWORD:password}@${RABBITMQ_HOST:localhost}}

this config accepts AMQP_URI as an environment variable, if provided RABBITMQ_* nested variables will not be used.

The environment variable value is interpreted as YAML, so it is possible to use rich types:

.. code-block:: yaml

    # foobar.yaml
    ...
    THINGS: ${A_LIST_OF_THINGS}

.. code-block:: shell

    $ A_LIST_OF_THINGS=[A,B,C] nameko run --config ./foobar.yaml <module>[:<ServiceClass>]

the parser for environment variables will pair all brackets.

.. code-block::  yaml

    # foobar.yaml
    LANDING_URL_TEMPLATE: ${LANDING_URL_TEMPLATE:https://example.com/{path}}

so the default value for this config will be https://example.com/{path}

Interacting with running services
---------------------------------

.. code-block:: pycon

    $ nameko shell

Launch an interactive python shell for working with remote nameko services. This is a regular interactive interpreter, with a special module ``n`` added
to the built-in namespace, providing the ability to make RPC calls and dispatch events.

Making an RPC call to "target_service":

.. code-block:: pycon

    $ nameko shell
    >>> n.rpc.target_service.target_method(...)
    # RPC response


Dispatching an event as "source_service":

.. code-block:: pycon

    $ nameko shell
    >>> n.dispatch_event("source_service", "event_type", "event_payload")

