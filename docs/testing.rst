Testing Services
================

Philosophy
----------

Nameko's conventions are designed to make testing as easy as possible. Services are likely to be small and single-purpose, and dependency injection makes it simple to replace and isolate pieces of functionality.

The examples below use `pytest <http://pytest.org/latest/>`_, which is what Nameko's own test suite uses, but the helpers are test framework agnostic.

Unit Testing
------------

Unit testing in Nameko usually means testing a single service in isolation -- i.e. without any or most of its dependencies.

The :func:`~nameko.testing.services.worker_factory` utility will create a worker from a given service class, with its dependencies replaced by :class:`mock.MagicMock` objects. Dependency functionality can then be imitated by adding :attr:`~mock.Mock.side_effect`\s and :attr:`~mock.Mock.return_value`\s:

.. literalinclude:: examples/testing/unit_test.py

In some circumstances it's helpful to provide an alternative dependency, rather than use a mock. This may be a fully functioning replacement (e.g. a test database session) or a lightweight shim that provides partial functionality.

.. literalinclude:: examples/testing/alternative_dependency_unit_test.py

Integration Testing
-------------------

Integration testing in Nameko means testing the interface between a number of services. The recommended way is to run all the services being tested in the normal way, and trigger behaviour by "firing" an entrypoint using a helper:

.. literalinclude:: examples/testing/integration_test.py

Note that the interface between ``ServiceX`` and ``ServiceY`` here is just as if under normal operation.

Interfaces that are out of scope for a particular test can be deactivated with one of the following test helpers:

restrict_entrypoints
^^^^^^^^^^^^^^^^^^^^

.. autofunction:: nameko.testing.services.restrict_entrypoints
   :noindex:

replace_dependencies
^^^^^^^^^^^^^^^^^^^^

.. autofunction:: nameko.testing.services.replace_dependencies
   :noindex:

Complete Example
^^^^^^^^^^^^^^^^

The following integration testing example makes use of both scope-restricting helpers:

.. literalinclude:: examples/testing/large_integration_test.py

Other Helpers
-------------

entrypoint_hook
^^^^^^^^^^^^^^^

The entrypoint hook allows a service entrypoint to be called manually. This is useful during integration testing if it is difficult or expensive to fake to external event that would cause an entrypoint to fire.

You can provide `context_data` for the call to mimic specific call context, for example language, user agent or auth token.

.. literalinclude:: examples/testing/entrypoint_hook_test.py

entrypoint_waiter
^^^^^^^^^^^^^^^^^

The entrypoint waiter is a context manager that does not exit until a named entrypoint has fired and completed. This is useful when testing integration points between services that are asynchronous, for example receiving an event:

.. literalinclude:: examples/testing/entrypoint_waiter_test.py

Note that the context manager waits not only for the entrypoint method to complete, but also for any dependency teardown. Dependency-based loggers such as (TODO: link to bundled logger) for example would have also completed.


Using pytest
------------

Nameko's test suite uses pytest, and makes some useful configuration and fixtures available for your own tests if you choose to use pytest.

They are contained in :mod:`nameko.testing.pytest`. This module is `automatically registered as a pytest plugin <https://pytest.org/latest/plugins.html#setuptools-entry-points>`_ by setuptools if you have pytest installed.
