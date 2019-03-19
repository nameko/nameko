:orphan:

Nameko
======

*[nah-meh-koh]*

.. pull-quote ::

    A microservices framework for Python that lets service developers concentrate on application logic and encourages testability.

A nameko service is just a class:

.. code-block:: python

    # helloworld.py

    from nameko.rpc import rpc

    class GreetingService:
        name = "greeting_service"

        @rpc
        def hello(self, name):
            return "Hello, {}!".format(name)

.. note::
    The example above requires `RabbitMQ <https://www.rabbitmq.com>`_, because
    it's using the built-in AMQP RPC features. `RabbitMQ installation guidelines <https://www.rabbitmq.com/download.html>`_
    offer several installation options, but you can quickly install and run it
    using `Docker <https://docs.docker.com/install/>`_.

    To install and run RabbitMQ using docker:

    .. code-block:: shell

       $ docker run -d -p 5672:5672 rabbitmq:3

    | *You might need to use sudo to do that.*

You can run it in a shell:

.. code-block:: shell

    $ nameko run helloworld
    starting services: greeting_service
    ...

And play with it from another:

.. code-block:: pycon

    $ nameko shell
    >>> n.rpc.greeting_service.hello(name="ナメコ")
    'Hello, ナメコ!'



User Guide
----------

This section covers most things you need to know to create and run your own Nameko services.

.. toctree::
   :maxdepth: 2

   what_is_nameko
   key_concepts
   installation
   cli
   built_in_extensions
   community_extensions
   testing
   writing_extensions



More Information
----------------

.. toctree::
   :maxdepth: 2

   about_microservices
   dependency_injection_benefits
   similar_projects
   getting_in_touch
   contributing
   license
   release_notes
   api
