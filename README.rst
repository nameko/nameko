Nameko
======

.. image:: https://secure.travis-ci.org/nameko/nameko.svg?branch=master
   :target: http://travis-ci.org/nameko/nameko

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
    >>> n.rpc.greeting_service.hello(name="ナメコ")
    'Hello, ナメコ!'


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

For help, comments or questions, please go to <https://discourse.nameko.io/>.

For enterprise
---------------------

Available as part of the Tidelift Subscription.

The maintainers of Nameko and thousands of other packages are working with Tidelift to deliver commercial support and maintenance for the open source dependencies you use to build your applications. Save time, reduce risk, and improve code health, while paying the maintainers of the exact dependencies you use. `Learn more. <https://tidelift.com/subscription/pkg/pypi-nameko?utm_source=pypi-nameko&utm_medium=referral&utm_campaign=enterprise&utm_term=repo>`_


Security contact information
----------------------------

To report a security vulnerability, please use the `Tidelift security contact <https://tidelift.com/security>`_. Tidelift will coordinate the fix and disclosure.


Contribute
----------

* Fork the repository
* Raise an issue or make a feature request


License
-------

Apache 2.0. See LICENSE for details.
