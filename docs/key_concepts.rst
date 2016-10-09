Key Concepts
============

This section introduces Nameko's central concepts.

Anatomy of a Service
--------------------

A Nameko service is just a Python class. The class encapsulates the application logic in its methods and declares any :ref:`dependencies <dependencies>` as attributes.

Methods are exposed to the outside world with :ref:`entrypoint <entrypoints>` decorators.

.. literalinclude:: examples/anatomy.py

.. _entrypoints:

Entrypoints
^^^^^^^^^^^

Entrypoints are gateways into the service methods they decorate. They  normally monitor an external entity, for example a message queue. On a relevant event, the entrypoint may "fire" and the decorated method would be executed by a service :ref:`worker <workers>`.

.. _dependencies:

Dependencies
^^^^^^^^^^^^

Most services depend on something other than themselves. Nameko encourages these things to be implemented as dependencies.

A dependency is an opportunity to hide code that isn't part of the core service logic. The dependency's interface to the service should be as simple as possible.

Declaring dependencies in your service is a good idea for :ref:`lots of reasons <benefits_of_dependency_injection>`, and you should think of them as the gateway between service code and everything else. That includes other services, external APIs, and even databases.

.. _workers:

Workers
^^^^^^^

Workers are created when an entrypoint fires. A worker is just an instance of the service class, but with the dependency declarations replaced with instances of those dependencies (see :ref:`dependency injection <dependency_injection>`).

Note that a worker only lives for the execution of one method -- services are stateless from one call to the next, which encourages the use of dependencies.

A service can run multiple workers at the same time, up to a user-defined limit. See :ref:`concurrency <concurrency>` for details.

.. _dependency_injection:

Dependency Injection
--------------------

Adding a dependency to a service class is declarative. That is, the attribute on the class is a declaration, rather than the interface that workers can actually use.

The class attribute is a :class:`~nameko.extensions.DependencyProvider`. It is responsible for providing an object that is injected into service workers.

Dependency providers implement a :meth:`~nameko.extensions.DependencyProvider.get_dependency` method, the result of which is injected into a newly created worker.

The lifecycle of a worker is:

    #. Entrypoint fires
    #. Worker instantiated from service class
    #. Dependencies injected into worker
    #. Method executes
    #. Worker is destroyed

In pseudocode this looks like::

    worker = Service()
    worker.other_rpc = worker.other_rpc.get_dependency()
    worker.method()
    del worker

Dependency providers live for the duration of the service, whereas the injected dependency can be unique to each worker.

.. _concurrency:

Concurrency
-----------

Nameko is built on top of the `eventlet <http://eventlet.net/>`_ library, which provides concurrency via "greenthreads". The concurrency model is co-routines with implicit yielding.

Implicit yielding relies on `monkey patching <http://eventlet.net/doc/patching.html#monkeypatching-the-standard-library>`_ the standard library, to trigger a yield when a thread waits on I/O. If you host services with ``nameko run`` on the command line, Nameko will apply the monkey patch for you.

Each worker executes in its own greenthread. The maximum number of concurrent workers can be tweaked based on the amount of time each worker will spend waiting on I/O.

Workers are stateless so are inherently thread safe, but dependencies should ensure they are unique per worker or otherwise safe to be accessed concurrently by multiple workers.

Note that many C-extensions that are using sockets and that would normally be considered thread-safe may not work with greenthreads. Among them are `librabbitmq <https://pypi.python.org/pypi/librabbitmq>`_, `MySQLdb <http://mysql-python.sourceforge.net/MySQLdb.html>` and others.

.. _extensions:

Extensions
----------

All entrypoints and dependency providers are implemented as "extensions". We refer to them this way because they're outside of service code but are not required by all services (for example, a purely AMQP-exposed service won't use the HTTP entrypoints).

Nameko has a number of :ref:`built-in extensions <built_in_extensions>`, some are :ref:`provided by the community <community_extensions>` and you can :ref:`write your own <writing_extensions>`.

.. _running_services:

Running Services
----------------

All that's required to run a service is the service class and any relevant configuration. The easiest way to run one or multiple services is with the Nameko CLI::

    $ nameko run module:[ServiceClass]

This command will discover Nameko services in the given ``module``\s and start running them. You can optionally limit it to specific ``ServiceClass``\s.

.. _containers:

Service Containers
^^^^^^^^^^^^^^^^^^

Each service class is delegated to a :class:`~nameko.containers.ServiceContainer`. The container encapsulates all the functionality required to run a service, and also encloses any :ref:`extensions <extensions>` on the service class.

Using the ``ServiceContainer`` to run a single service:

.. literalinclude:: examples/service_container.py

.. _runner:

Service Runner
^^^^^^^^^^^^^^

:class:`~nameko.runners.ServiceRunner` is a thin wrapper around multiple containers, exposing methods for starting and stopping all the wrapped containers simultaneously. This is what ``nameko run`` uses internally, and it can also be constructed programmatically:

.. literalinclude:: examples/service_runner.py
