Nameko
======

.. image:: https://secure.travis-ci.org/onefinestay/nameko.png?branch=master
   :target: http://travis-ci.org/onefinestay/nameko

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


You can run it in a shell:

.. code-block:: shell

    $ nameko run helloworld
    starting services: greeting_service
    ...

And play with it from another:

.. code-block:: pycon

    $ nameko shell
    >>> n.rpc.greeting_service.hello(name="Matt")
    'Hello, Matt!'


Features
--------

* AMQP RPC and Events (pub-sub)
* HTTP GET, POST & websockets
* CLI for easy and rapid development
* Utilities for unit and integration testing


Getting Started
---------------

* Check out the `documentation <http://nameko.readthedocs.io>`_.


Support
-------

For help, comments or questions, please use the `mailing list
<https://groups.google.com/forum/#!forum/nameko-dev>`_ on google groups.


Contribute
----------

* Fork the repository
* Raise an issue or make a feature request


License
-------

Apache 2.0. See LICENSE for details.
