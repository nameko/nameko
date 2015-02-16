.. _benefits_of_dependency_injection:

Benefits of Dependency Injection
--------------------------------

The dependency injection pattern in nameko facilitates a separation of concerns between component parts of a service. There is a natural division between "service code" -- application logic tied to the :ref:`single-purpose <single_purpose>` of the service -- and the rest of the code required for the service to operate.

Say you had a caching service that was responsible for reading and writing from memcached, and included some business-specific invalidation rules. The invalidation rules are clearly application logic, whereas the messy network interface with memcached can be abstracted away into a dependency.

Separating these concerns makes it easier to test service code in isolation. That means you don't need to have a memcached cluster available when you test your caching service. Furthermore it's easy to specify mock responses from the memcached cluster to test invalidation edge cases.

Separation also stops test scopes bleeding into one another. Without a clear interface between the caching service and the machinery it uses to communicate with memcached, it would be tempting to cover network-glitch edge cases as part of the caching service test suite. In fact the tests for this scenario should be as part of the test suite for the memcached dependency. This becomes obvious when dependencies are used by more than one service -- without a separation you would have to duplicate the network-glitch tests or seem to have holes in your test coverage.

A more subtle benefit manifests in larger teams. Dependencies tend to encapsulate the thorniest and most complex aspects of an application. Whereas service code is stateless and single-threaded, dependencies must deal with concurrency and thread-safety. This can be a helpful division of labour between junior and senior developers.

Dependencies separate common functionality away from bespoke application logic. They can be written once and re-used by many services. Nameko's :ref:`community extensions <community_extensions>` aims to promote sharing even between teams.
