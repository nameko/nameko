About Microservices
===================

    An approach to designing software as a suite of small services, each running in its own process and communicating with lightweight mechanisms.

    -- Martin Fowler, `Microservices Architecture <http://martinfowler.com/articles/microservices.html>`_

Microservices are usually described in contrast to a "monolith" -- an application built as a single unit where changes to any part of it require building and deploying the whole thing.

With microservices, functionality is instead split into "services" with well defined boundaries. Each of these services can be developed and deployed individually.

Benefits
--------

    * Individually deployable
    * Don't have to understand the whole thing / easier to split up work
    * Easier to build in fault tolerance
    * Clear "published interface"
    * Enable different languages / specialisation
    * Enable different teams (conway's law)

Drawbacks
---------

    * Relatively new approach
    * RPC calls are more expensive
    * Forces course APIs
    * No cross-service transactions
    * Eventual consistency
    * Harder to track dependencies - change to one service impacts others

Further Notes
-------------

You can adopt microservices next to a monolith.
