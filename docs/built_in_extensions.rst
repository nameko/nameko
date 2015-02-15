.. _built_in_extensions:

Built-in Extensions
===================

RPC
---

Nameko has a built-in implementation for RPC over AMQP. It includes the ``@rpc`` entrypoint, a proxy for services to talk to other services, and a standalone proxy that non-nameko clients can use to make RPC calls to a cluster:

.. literalinclude:: examples/rpc.py

.. literalinclude:: examples/standalone_rpc.py

Normal RPC calls block until the remote method completes, but proxies also have an asynchronous calling mode to background or parallelize RPC calls:

.. literalinclude:: examples/async_rpc.py

In a cluster with more than one instance of the target service, RPC requests round-robin between instances. The request will be handled by exactly one instance of the target service.

AMQP messages are ack'd only after the request has been successfully processed. If the service fails to acknowledge the message and the AMQP connection is closed (e.g. if the service process is killed) the broker will revoke and then allocate the message to the available service instance.

Request and response payloads are serialized into JSON for transport over the wire.

Events (Pub-Sub)
----------------

Nameko Events is an asynchronous messaging system, implementing the Publish-Subscriber pattern. Services dispatch events that may be received by zero or more others:

.. literalinclude:: examples/events.py

The :class:`~nameko.events.EventHandler` entrypoint has three ``handler_type``\s that determine how event messages are received in a cluster:

    * ``SERVICE_POOL`` -- event handlers are pooled by service name and one instance from each pool receives the event, similar to the cluster behaviour of the RPC entrypoint. This is the default handler type.
    * ``BROADCAST`` -- every listening service instance will receive the event.
    * ``SINGLETON`` -- exactly one listening service instance will receive the event.

Events are serialized into JSON for transport over the wire.

HTTP GET & POST
---------------

Nameko's HTTP entrypoint supports simple GET and POST:

.. literalinclude:: examples/http.py

::

    $ nameko run http
    starting services: httpservice

::

    $ curl -i localhost:8000/get/42
    HTTP/1.1 200 OK
    Content-Type: text/plain; charset=utf-8
    Content-Length: 13
    Date: Fri, 13 Feb 2015 14:51:18 GMT

    {'value': 42}

::

    $ curl -i -d "post body" localhost:8000/post
    HTTP/1.1 200 OK
    Content-Type: text/plain; charset=utf-8
    Content-Length: 19
    Date: Fri, 13 Feb 2015 14:55:01 GMT

    received: post body


The HTTP entrypoint is built on top of `werkzeug <http://werkzeug.pocoo.org/>`_. Service methods can return specific status codes and headers, or a :class:`werkzeug.wrappers.Response` object:

.. literalinclude:: examples/advanced_http.py

::

    $ nameko run advanced_http
    starting services: advancedhttpservice

::

    $ curl -i localhost:8000/privileged
    HTTP/1.1 403 FORBIDDEN
    Content-Type: text/plain; charset=utf-8
    Content-Length: 9
    Date: Fri, 13 Feb 2015 14:58:02 GMT

::

    curl -i localhost:8000/headers
    HTTP/1.1 201 CREATED
    Location: https://www.example.com/widget/1
    Content-Type: text/plain; charset=utf-8
    Content-Length: 0
    Date: Fri, 13 Feb 2015 14:58:48 GMT


Websockets
----------

Nameko includes *experimental* support for RPC and broadcast to subscribers over websockets.

.. literalinclude:: examples/websocket_rpc.py

::

    $ nameko run websockets
    starting services: websocketrpc

Using `wscat <https://www.npmjs.com/package/wscat>`_ in a terminal:

::

    $ wscat -c ws://localhost:8000/ws
    connected (press CTRL+C to quit)
      < {"data": {"socket_id": "9110b890-9456-4d75-8dab-2edf20d41f15"}, "type": "event", "event": "connected"}
    > {"method":"echo", "data":{"value":"hello websockets"}, "correlation_id":1}
      < {"data": "hello websockets", "correlation_id": 1, "type": "result", "success": true}

Timer
-----

The :class:`~nameko.timers.Timer` is a simple entrypoint that fires once per a configurable number of seconds. The timer is not "cluster-aware" and fires on all services instances.

.. literalinclude:: examples/timer.py
