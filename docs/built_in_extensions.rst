Built-in Extensions
===================

RPC
---

Nameko has a built-in implementation for RPC over AMQP. It includes the ``@rpc`` entrypoint, a proxy for services to talk to other services, and a standalone proxy that clients can use to make RPC calls to a cluster:

.. literalinclude:: examples/rpc.py

.. literalinclude:: examples/standalone_rpc.py

Normal RPC calls block until the remote method completes, but proxies also have an "async" calling mode to background or parallelize RPC calls:

.. literalinclude:: examples/async_rpc.py


Events (Pub-Sub)
----------------

Nameko Events is an asynchronous messaging system, similar to the Publish-Subscriber pattern. Services dispatch events that may be received by zero or more others:

.. literalinclude:: examples/events.py

There is also a standalone dispatcher that can be used to imitate an event being dispatched from a service:

.. literalinclude:: examples/standalone_events.py


HTTP GET & POST
---------------

Nameko's HTTP entrypoint supports simple GET and POST:

.. literalinclude:: examples/http.py

Service methods can return specific status codes and set headers:

.. literalinclude:: examples/advanced_http.py


Websockets
----------

Nameko's websocket implementation allows for RPC over a websocket and a facility to broadcast messages to subscribed listeners:

(TODO)
