Command Line Interface
======================

Nameko ships with a command line interface to make hosting and interacting with services as easy as possible.

Running a Service
-----------------

::

    $ nameko run <module>[:<ServiceClass>]

Discover and run a service class. This will start the service in the foreground and run it until the process terminates.


Interacting with running services
---------------------------------

::

    $ nameko shell

Launch an interactive python shell for working with remote nameko services. This is a regular interactive interpreter, with a special module ``n`` added
to the built-in namespace, providing the ability to make RPC calls and dispatch events.

Making an RPC call to "target_service":

::

    Nameko Python 2.7.8 (default, Oct 19 2014, 16:02:00)
    [GCC 4.2.1 Compatible Apple LLVM 6.0 (clang-600.0.54)] shell on darwin
    Broker: amqp://guest:guest@localhost:5672/nameko
    >>> n.rpc.target_service.target_method(...)
    # RPC response


Dispatching an event as "source_service":

::

    Nameko Python 2.7.8 (default, Oct 19 2014, 16:02:00)
    [GCC 4.2.1 Compatible Apple LLVM 6.0 (clang-600.0.54)] shell on darwin
    Broker: amqp://guest:guest@localhost:5672/nameko
    >>> n.dispatch_event("source_service", "event_type", "event_payload")
