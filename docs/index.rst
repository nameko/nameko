:orphan:

Nameko
======

*[nah-meh-koh]*

.. pull-quote ::

    A microservices framework for Python that lets service developers concentrate on application logic and encourages testability.

A nameko service is just a class:

.. code-block:: python

    from nameko.rpc import rpc

    class HelloWorld(object):

        @rpc
        def hello(self, name):
            return "Hello, {}!".format(name)


You can run it in a shell:

::

    $ nameko run helloworld:HelloWorld
    INFO:nameko.runners:starting services: ['helloworld']
    INFO:kombu.mixins:Connected to amqp://guest@127.0.0.1:5672/nameko
    INFO:nameko.runners:services started: ['helloworld']
    ...

And play with it from another:

::

    $ nameko shell
    Nameko Python 2.7.8 (default, Oct 19 2014, 16:02:00)
    [GCC 4.2.1 Compatible Apple LLVM 6.0 (clang-600.0.54)] shell on darwin
    Broker: amqp://guest:guest@localhost:5672/nameko
    >>> n.rpc.helloworld.hello(name="Matt")
    u'Hello, Matt!'



User Guide
----------

This section covers most things you need to know to create and run your own nameko services.

.. toctree::
   :maxdepth: 2

   what_is_nameko
   key_concepts
   installation
   cli
   built_in_extensions
   testing
   writing_dependencies


More Information
----------------

.. toctree::
   :maxdepth: 2

   about_microservices
   dependency_injection_benefits
   similar_projects
   license


API Reference
-------------

.. toctree::
   :maxdepth: 2

   api
