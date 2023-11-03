.. _upgrading-to-nameko-3:

Upgrading to Nameko 3
=====================

What's new in Nameko 3
----------------------

Configuration improvements
~~~~~~~~~~~~~~~~~~~~~~~~~

* Configuration loaded from a file or declared in the CLI is now available in a :ref:`global config object <nameko-config>`.
* Configuration may now be :ref:`declared on the CLI <cli-define-option>`.
* Configuration access via the service container is now deprecated.
* Builtin extensions and other facilities that read the configuration now access it via the global.

CLI improvements
~~~~~~~~~~~~~~~~

* The CLI has been rebuilt using `click <http://click.pocoo.org/>`_ to be more robust, reliable and extensible..
* The `backdoor` command has been removed; `run`` informs about how to connect to the backdoor port.

AMQP refactors
~~~~~~~~~~~~~~

The “built-in” extensions that use AMQP (RPC, Events) have been refactored. The API is backwards compatible but enhanced in many ways:

* `prefetch_count` is exposed as a first-class parameter, rather than inheriting max_workers.
* Publisher options can be provided to the EventDispatcher and Publisher.
* ...

Configuration improvements
--------------------------

.. _nameko-config:

Global config object
~~~~~~~~~~~~~~~~~~~~

The configuration is now available in a global object, rather than being passed around in the service container. This means that configuration can be accessed from anywhere, including from outside the service container.

The new `nameko.config` dictionary allows you to access service configuration at any time, including service class definition:

.. code-block:: python

    from nameko.messaging import Consumer
    from nameko import config

    class Service:

        @consume(
            queue=Queue(
                exchange=config["MY_EXCHANGE"],
                routing_key=config["MY_ROUTING_KEY"],
                name=config["MY_QUEUE_NAME"]
            ),
            prefetch_count=config["MY_CONSUMER_PREFETCH_COUNT"]
        )
        def consume(self, payload):
            pass

As you can see having config available outside service container also helps with configuring entrypoints and dependency providers by passing options to their constructors:

.. code-block:: python

    from nameko import config
    from nameko_sqlalchemy import Database

    class Service:
        db = Database(engine_options=config["DB_ENGINE_OPTIONS"])

.. _cli-define-option:

Declaration of config in the CLI
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Configuration can now be declared on the CLI, rather than only being loaded from a file. This is useful for setting configuration that is specific to the environment in which the service is running, such as the hostname of a database server.

Define config entries on the fly when running a service or shell:

.. code-block:: bash

    $ nameko run service \
    --define AMQP_URI=pyamqp://someuser:*****@somehost/ \
    --define max_workers=1000 \
    --define SLACK="{'TOKEN': '*****'}"


Upgrade Guide
-------------

Upgrading services
~~~~~~~~~~~~~~~~~~

This guide covers the steps to modify an existing service so that it works with Nameko 3.

Generally only three areas are affected:

1. :ref:`using-config-in-service-code`
2. :ref:`using-config-in-tests`
3. :ref:`using-custom-runners`

.. _using-config-in-service-code:

Using config in service code
................................

**Optional** — Use the global `config`` object rather than `Config`` dependency provider.
The built-in `Config`` dependency provider from `nameko.dependency_providers` has been deprecated. You should instead use the `nameko.config` global.

**Optional** — Use the global `config` object to simplify service setup.
The global is available at all stages of the service lifecycle, including import time, which can make it much simpler to configure services.

**Required ⚠** — Don't pass `config`` to `ServiceRpcProxy` or `ClusterRpcProxy`
If you are using these standalone RPC clients from the `nameko.standalone` package, note that they no longer accept `config` as a parameter.

You must set up config in the global context before using them, for example with `nameko.config.patch`.

.. _using-config-in-tests:

Using config in tests
.....................

**Required ⚠** — Read global `config` rather than any of the `x_config`` fixtures.
There is a breaking change to the way that the `rabbit_config`, `test_config` and `empty_config` fixtures work. They no longer return the config dictionary.

If you are using one of these fixtures, see the recommended pattern in the detailed breakdown of these configuration changes.

**Optional** — Don’t pass config to container_factory or runner_factory.
If you are using these pytest fixtures, note that the config argument is now deprecated.

See the detailed breakdown of these changes for more information.

.. _using-custom-runners:

Using custom runners
....................

Required ⚠ — Don’t pass config to ServiceContainer or ServiceRunner
In Nameko 3, the ServiceContainer and ServiceRunner classes do not accept a config argument anymore. If you are programmatically creating a service runner, rather than using nameko run in the CLI, you must set up config in the global context before using them, for example with nameko.config.patch.

See the detailed breakdown of these changes for more information.

Built-in RPC clients
....................

Note that the following dependency providers have been renamed:

* `ClusterRpcProxy` -> `ClusterRpcClient`
* `ServiceRpcProxy` -> `ServiceRpcClient`

The old names are preserved for backwards compatibility.

Upgrading Extensions
~~~~~~~~~~~~~~~~~~~~

Nameko 3 is backwards-compatible so that extensions written for Nameko 2 should not require any modification in order to work with Nameko 3.

In particular:

The `container_factory` and `runner_factory` fixtures still accept config, even as a deprecated argument. The `ServiceContainer.config` attribute still returns a reference to the config object. This is also deprecated and will raise a warning.
Before Nameko 2 is sunsetted, extensions should make the necessary adjustments to read from the global config object.

Test Upgrades
.............

Although extensions will be backwards-compatible when used in services running Nameko 3, some changes may be required to their tests. These are the same changes that apply when :ref:`reading config inside tests for service code <using-config-in-tests>`.


Configuration changes
---------------------

`ServiceContainer.config` is deprecated. Accessing the config via `ServiceContainer.config` is deprecated in favour of using the global config object, but continues to exist in order to preserve backwards compatibility.

Accessing config via the global:

.. code-block:: python

    from nameko import config
    from nameko.extensions import DependencyProvider

    class Database(DependencyProvider):
        def setup(self):
            db_uris = config[DB_URIS_KEY]  # <- Nameko 3 way, won't work with Nameko 2!
            # ...

Accessing config via the service container:

.. code-block:: python

    from nameko.extensions import DependencyProvider

    class Database(DependencyProvider):
        def setup(self):
            db_uris = self.container.config[DB_URIS_KEY]  # <- still works, Nameko 2 & 3
            # ...

In Nameko 3, the above will result in a `DeprecationWarning`:

.. code-block:

    .../nameko/containers.py:173: DeprecationWarning: Use ``nameko.config`` instead.
    warnings.warn("Use ``nameko.config`` instead.", DeprecationWarning)


Programmatically running services
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. warning:: Breaking Change!

    The following section contains a breaking change.

This section is relevant if you are creating runner programmatically, rather than using the `run` CLI command.

In Nameko 3, the `ServiceContainer` and `ServiceRunner` classes do not accept the `config` argument anymore. You must update configuration in the global object, with `config.patch` or directly:

.. code-block:: python

    import nameko
    from nameko.containers import ServiceContainer

    nameko.config["AMQP_URI"] = "pyamqp://someuser:*****@somehost/"

    container = ServiceContainer(Service)
    container.start()
    # AMQP extensions will connect as someuser to RabbitMQ broker at somehost ...
    container.stop()

Same for the runner:

.. code-block:: python

    from nameko.runners import ServiceRunner

    nameko.config["AMQP_URI"] = "pyamqp://someuser:*****@somehost/"

    runner = ServiceRunner()
    runner.add_service(ServiceA)
    runner.add_service(ServiceB)

    runner.start()
    # AMQP extensions will connect as someuser to RabbitMQ broker at somehost ...
    runner.stop()

Other configuration related changes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* The `--broker`` CLI option is deprecated in favour of `--define` and `--config`. TODO: doesn’t actually raise a warning anymore
* The built-in Config dependency provider is deprecated as the config can be accessed and read directly.

Testing services
----------------

In Nameko 2, configuration was always explicitly passed as an object, which made it easy to manage in tests. The global config object needs some different ways of managing configuration in tests.

Config patching
~~~~~~~~~~~~~~~

The global `config` object has a `patch` helper object which can be used as a context manager or as a decorator. Here is an example of patching the config object in a pytest fixture:

.. code-block:: python

    import nameko
    import pytest

    @pytest.fixture
    def memory_rabbit_config():
        with nameko.config.patch({"AMQP_URI": "memory://"}):
            yield

While this fixture is in place, any code accessing `config['AMQP_URI']` will receive the value `"memory://"`.

Changes to the `container_factory` and `runner_factory` fixtures.

In Nameko 2, the `container_factory` and `runner_factory` fixtures accepted `config` as a required argument. In Nameko 3, it is optional. If passed, `nameko.config.patch` is used to wrap running container. The patch is applied before container start and returned it back to its original value when exiting the test:

.. code-block:: python

    def test_with_container_factory(container_factory):
        container = container_factory(Service, config={"AMQP_URI": "..."})
        container.start()  # <- starts config patching scope, ending it when container
                        #    stops (exit of the container_factory fixture)
        # ...

    def test_with_runner_factory(runner_factory):
        runner = runner_factory(config={"AMQP_URI": "..."}, ServiceX, ServiceY)
        runner.start()  # <- starts config patching scope, ending it when runner
                        #    stops (exit of the runner_factory fixture)
        # ...

The optional argument provides backward compatibility so that tests for Nameko extensions can be compatible with both major versions.

When creating or upgrading existing services to run on Nameko 3, it is recommended to use the new config patch pattern instead:

.. code-block:: python

    import nameko

    @nameko.config.patch({"AMQP_URI": "memory://"})
    def test_spam():
        container = container_factory(Service)
        container.start()

    @nameko.config.patch({"AMQP_URI": "memory://"})
    def test_service_x_y_integration(runner_factory):
        runner = runner_factory(ServiceX, ServiceY)
        runner.start()

    @pytest.mark.usefixtures('rabbit_config')
    def test_spam():
        container = container_factory(Service)
        container.start()

    @pytest.mark.usefixtures('rabbit_config')
    def test_service_x_y_integration(runner_factory):
        runner = runner_factory(ServiceX, ServiceY)
        runner.start()

Changes to the `*_config`` fixtures
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. warning:: Breaking Change!

    The following section contains a breaking change.

* `empty_config`
* `rabbit_config`
* `web_config`

These fixtures no longer return the config dictionary. They simply add their configuration values to the existing `config` object.

A recommended pattern for setting up a config object that draws from these fixtures is as follows:

.. code-block:: python

    import nameko
    import pytest

    @pytest.fixture(autouse=True)
    def config(web_config, rabbit_config):
        config = {
            # some custom testing config, defined in place or loaded from file ...
        }
        config.update(web_config)
        config.update(rabbit_config)
        with nameko.config.patch(config):
            yield