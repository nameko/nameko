Command Line Interface
======================

Nameko ships with a command line interface to make hosting and interacting with services as easy as possible.

Running a Service
-----------------

.. code-block:: shell

    $ nameko run <module>[:<ServiceClass>]

Discover and run a service class. This will start the service in the foreground and run it until the process terminates.


Interacting with running services
---------------------------------

.. code-block:: pycon

    $ nameko shell

Launch an interactive python shell for working with remote nameko services. This is a regular interactive interpreter, with a special module ``n`` added
to the built-in namespace, providing the ability to make RPC calls and dispatch events.

Making an RPC call to "target_service":

.. code-block:: pycon

    $ nameko shell
    >>> n.rpc.target_service.target_method(...)
    # RPC response


Dispatching an event as "source_service":

.. code-block:: pycon

    $ nameko shell
    >>> n.dispatch_event("source_service", "event_type", "event_payload")
