Nameko
======

.. image:: https://secure.travis-ci.org/onefinestay/nameko.png?branch=master
   :target: http://travis-ci.org/onefinestay/nameko

*[nah-meh-koh]*

.. pull-quote ::

    A microservices framework for Python that lets service developers concentrate on application logic and encourages testability.


A Nameko service is just a class:

.. code-block:: python

    # helloworld.py

    from nameko.rpc import rpc

    class HelloWorld(object):

        @rpc
        def hello(self, name):
            return "Hello, {}!".format(name)


You can run it in a shell:

.. code-block:: shell

    $ nameko run helloworld
    starting services: helloworld
    ...

And play with it from another:

.. code-block:: pycon

    $ nameko shell
    >>> n.rpc.helloworld.hello(name="Matt")
    u'Hello, Matt!'


Features
--------

    * AMQP RPC and Events (pub-sub)
    * HTTP GET, POST & websockets
    * CLI for easy and rapid development
    * Utilities for unit and integration testing


Getting Started
---------------

    * `Download <https://pypi.python.org/packages/source/n/nameko/nameko-1.14.0.tar.gz#md5=fca6606fdd38d325ad96a40a383e035d>`_ the latest version.
    * Check out the `documentation <http://nameko.readthedocs.org>`_.


Support
-------

    * Join the mailing list
    * Find us on IRC


Contribute
----------

    * Fork the repository
    * Raise an issue or make a feature request


License
-------

Apache 2.0. See LICENSE for details.
