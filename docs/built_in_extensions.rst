Built-in Extensions
===================

RPC
---

Nameko has a built-in implementation for RPC over AMQP. It includes the ``@rpc`` entrypoint, a proxy for services to talk to other services, and a standalone proxy that non-nameko clients can use to make RPC calls to a cluster:

.. literalinclude:: examples/rpc.py

.. literalinclude:: examples/standalone_rpc.py

Normal RPC calls block until the remote method completes, but proxies also have an asynchronous calling mode to background or parallelize RPC calls:

.. literalinclude:: examples/async_rpc.py


Events (Pub-Sub)
----------------

Nameko Events is an asynchronous messaging system, implementing the Publish-Subscriber pattern. Services dispatch events that may be received by zero or more others:

.. literalinclude:: examples/events.py


HTTP GET & POST
---------------

Nameko's HTTP entrypoint supports simple GET and POST:

.. literalinclude:: examples/http.py

The HTTP entrypoint is built on top of `werkzeug <http://werkzeug.pocoo.org/>`_. Service methods can return specific status codes and set headers, or a :class:`werkzeug.wrappers.Response` object:

.. literalinclude:: examples/advanced_http.py



Websockets
----------

Nameko includes experimental support for RPC and broadcast to subscribers over websockets.

This feature isn't yet extensively tested or used.
