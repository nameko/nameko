.. _built_in_extensions:

Built-in Extensions
===================

Nameko includes a number of built-in :ref:`extensions <extensions>`. This section introduces them and gives brief examples of their usage.

RPC
---

Nameko includes an implementation of RPC over AMQP. It comprises the ``@rpc`` entrypoint, a proxy for services to talk to other services, and a standalone proxy that non-Nameko clients can use to make RPC calls to a cluster:

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

An example of using the ``BROADCAST`` mode:

.. literalinclude:: examples/event_broadcast.py

Events are serialized into JSON for transport over the wire.

HTTP GET & POST
---------------

Nameko's HTTP entrypoint supports simple GET and POST:

.. literalinclude:: examples/http.py

.. code-block:: shell

    $ nameko run http
    starting services: http_service

.. code-block:: shell

    $ curl -i localhost:8000/get/42
    HTTP/1.1 200 OK
    Content-Type: text/plain; charset=utf-8
    Content-Length: 13
    Date: Fri, 13 Feb 2015 14:51:18 GMT

    {'value': 42}

.. code-block:: shell

    $ curl -i -d "post body" localhost:8000/post
    HTTP/1.1 200 OK
    Content-Type: text/plain; charset=utf-8
    Content-Length: 19
    Date: Fri, 13 Feb 2015 14:55:01 GMT

    received: post body


The HTTP entrypoint is built on top of `werkzeug <http://werkzeug.pocoo.org/>`_. Service methods must return one of:

- a string, which becomes the response body
- a 2-tuple ``(status code, response body)``
- a 3-tuple ``(status_code, headers dict, response body)``
- an instance of :class:`werkzeug.wrappers.Response`

.. literalinclude:: examples/advanced_http.py

.. code-block:: shell

    $ nameko run advanced_http
    starting services: advanced_http_service

.. code-block:: shell

    $ curl -i localhost:8000/privileged
    HTTP/1.1 403 FORBIDDEN
    Content-Type: text/plain; charset=utf-8
    Content-Length: 9
    Date: Fri, 13 Feb 2015 14:58:02 GMT

.. code-block:: shell

    curl -i localhost:8000/headers
    HTTP/1.1 201 CREATED
    Location: https://www.example.com/widget/1
    Content-Type: text/plain; charset=utf-8
    Content-Length: 0
    Date: Fri, 13 Feb 2015 14:58:48 GMT


You can control formatting of errors returned from your service by overriding :meth:`~nameko.web.HttpRequestHandler.response_from_exception`:

.. literalinclude:: examples/http_exceptions.py

.. code-block:: shell

    $ nameko run http_exceptions
    starting services: http_service

.. code-block:: shell

    $ curl -i http://localhost:8000/custom_exception
    HTTP/1.1 400 BAD REQUEST
    Content-Type: application/json
    Content-Length: 72
    Date: Thu, 06 Aug 2015 09:53:56 GMT

    {"message": "Argument `foo` is required.", "error": "INVALID_ARGUMENTS"}



Timer
-----

The :class:`~nameko.timers.Timer` is a simple entrypoint that fires once per a configurable number of seconds. The timer is not "cluster-aware" and fires on all services instances.

.. literalinclude:: examples/timer.py
