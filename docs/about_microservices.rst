About Microservices
===================

    An approach to designing software as a suite of small services, each running in its own process and communicating with lightweight mechanisms.

    -- Martin Fowler, `Microservices Architecture <http://martinfowler.com/articles/microservices.html>`_

Microservices are usually described in contrast to a "monolith" -- an application built as a single unit where changes to any part of it require building and deploying the whole thing.

With microservices, functionality is instead split into "services" with well defined boundaries. Each of these services can be developed and deployed individually.

There are many benefits as well as drawbacks to using microservices, eloquently explained in Martin Fowler's `paper <http://martinfowler.com/articles/microservices.html>`_. Not all of them always apply, so below we'll outline some that are relevant to Nameko.

Benefits
--------

.. _single_purpose:

* Small and single-purpose

    Breaking a large application into smaller loosely-coupled chunks reduces the cognitive burden of working with it. A developer working on one service isn't required to understand the internals of the rest of the application; they can rely on a higher-level interface of the other services.

    This is harder to achieve in a monolithic application where the boundaries and interfaces between "chunks" are fuzzier.

* Explicit `published interface <http://martinfowler.com/bliki/PublishedInterface.html>`_

    The entrypoints for a Nameko service explicitly declare its published interface. This is the boundary between the service and its callers, and thus the point beyond which backwards compatibility must be considered or maintained.

* Individually deployable

    Unlike a monolith which can only be released all at once, Nameko services can be individually deployed. A change in one service can be made and rolled out without touching any of the others. Long running and highly considered release cycles can be broken into smaller, faster iterations.

* Specialization

    Decoupled chunks of application are free to use specialized libraries and dependencies. Where a monolith might be forced to choose a one-size-fits-all library, microservices are unshackled from the choices made by the rest of the application.


Drawbacks
---------

* Overhead

    RPC calls are more expensive than in-process method calls. Processes will spend a lot of time waiting on I/O. Nameko mitigates wastage of CPU cycles with concurrency and eventlet, but the latency of each call will be longer than in a monolithic application.

* Cross-service transactions

    Distributing transactions between multiple processes is difficult to the point of futility. Microservice architectures work around this by changing the APIs they expose (see below) or only offering eventual consistency.

* Coarse-grained APIs

    The overhead and lack of transactions between service calls encourages coarser APIs. Crossing service boundaries is expensive and non-atomic.

    Where in a monolithic application you might write code that makes many calls to achieve a certain goal, a microservices architecture will encourage you to write fewer, heavier calls to be atomic or minimize overhead.

* Understanding interdependencies

    Splitting an application over multiple separate components introduces the requirement to understand how those components interact. This is hard when the components are in different code bases (and developer head spaces).

    In the future we hope to include tools in Nameko that make understanding, documenting and visualizing service interdependencies easier.

Further Notes
-------------

Microservices can be adopted incrementally. A good approach to building a microservices architecture is to start by pulling appropriate chunks of logic out of a monolithic application and turning them into individual services.
