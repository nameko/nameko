What is Nameko?
===============

Nameko is a framework for building microservices in Python.

It comes with built-in support for:

    * RPC over AMQP
    * Asynchronous events (pub-sub) over AMQP
    * Simple HTTP GET and POST
    * Websocket RPC and subscriptions (experimental)

Out of the box you can build a service that can respond to RPC messages, dispatch events on certain actions, and listen to events from other services. It could also have HTTP interfaces for clients that can't speak AMQP, and a websocket interface for, say, Javascript clients.

Nameko is also extensible. You can define your own transport mechanisms and service dependencies to mix and match as desired.

Nameko strongly encourages the :ref:`dependency injection <benefits_of_dependency_injection>` pattern, which makes building and testing services clean and simple.

Nameko takes its name from the Japanese mushroom, which grows in clusters.


When should I use Nameko?
-------------------------

Nameko is designed to help you create, run and test microservices. You should use Nameko if:

    * You want to write your backend as microservices, or
    * You want to add microservices to an existing system, and
    * You want to do it in Python.

Nameko scales from a single instance of a single service, to a cluster with many instances of many different services.

The library also ships with tools for clients, which you can use if you want to write Python code to communicate with an existing Nameko cluster.


When shouldn't I use Nameko?
----------------------------

Nameko is not a web framework. It has built-in HTTP support but
it's limited to what is useful in the realm of microservices. If you want to build a webapp for consumption by humans you should use something like `flask <http://flask.pocoo.org>`_.
