Key Concepts
============

This section introduces nameko's central concepts.

Anatomy of a Service
--------------------

A nameko service is just a Python class. The class encapsulates the application logic in its methods and declares any :ref:`dependencies <dependencies>` as attributes.

Methods are exposed to the outside world with :ref:`entrypoint <entrypoints>` decorators.

.. literalinclude:: examples/example.py

.. _entrypoints:

Entrypoints
^^^^^^^^^^^

Entrypoints are gateways into the service methods they decorate. While a service is running, an entrypoint may "fire" and the decorated method would be executed by a service :ref:`worker <workers>`.

.. _dependencies:

Dependencies
^^^^^^^^^^^^

Most services depend on something other than themselves. Nameko encourages these things to be implemented as dependencies.

A dependency is an opportunity to hide code that isn't part of the core service logic. The dependency's interface to the service should be as simple as possible.

Declaring dependencies in your service is a good idea for :ref:`lots of reasons <benefits_of_dependency_injection>`, and you should think of them as the gateway between service code and everything else. That includes other services, external APIs, and even databases.

.. _workers:

Workers
^^^^^^^

Workers are created when an entrypoint fires. A worker is just an instance of the service class, but with interfaces to its dependencies injected into it.

Note that a worker only lives for the execution of one method - services are stateless from one call to the next, which encourages the use of dependencies.

A service can run multiple workers at the same time, up to a user-defined limit. See :ref:`concurrency` for details.

.. _dependency_injection:

Dependency Injection
--------------------

Adding a dependency to a service class is declarative. That is, the attribute on the class is a declaration, rather than the interface that workers can actually use.

The class attribute is a :class:`~nameko.extensions.DependencyProvider`. It is responsible for providing an object that is injected into service workers.

DependencyProviders implement a :meth:`~nameko.extensions.DependencyProvider.get_dependency` method, the result of which is injected into a newly created worker.

The lifecycle of a worker is:

    #. Entrypoint fires
    #. Worker instantiated from service class
    #. Dependencies injected into worker
    #. Method executes
    #. Worker is destroyed

In code this looks something like::

    worker = Service()
    worker.other_rpc = worker.other_rpc.get_dependency()
    worker.method()
    del worker

DependencyProviders live for the duration of the service, whereas the injected dependency can be unique to each worker.

.. _concurrency:

Concurrency
-----------

Nameko is built on top of the `eventlet <http://eventlet.net/>`_ library, which provides concurrency via "greenthreads". The concurrency model is co-routines with implicit yielding.

Implicit yielding relies on `monkey patching <http://eventlet.net/doc/patching.html#monkeypatching-the-standard-library>`_ the standard library, to trigger a yield when a thread waits on I/O. If you host services with ``nameko run`` on the command line, nameko will apply the monkey patch for you.

Each worker executes in its own greenthread. The maximum number of concurrent workers can be tweaked based on the amount of time each worker will spend waiting on I/O.

Workers are stateless so are inherently thread safe, but dependencies should ensure they support thread-safety.




