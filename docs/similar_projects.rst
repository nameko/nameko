Similar and Related Projects
============================

Celery
------

`Celery <http://celery.readthedocs.io/>`_ is a distributed task queue. It lets you define "tasks" as Python functions and execute them on a set of remote workers, not unlike Nameko RPC.

Celery is usually used as an add-on to existing applications, to defer processing or otherwise outsource some work to a remote machine. You could also achieve this with Nameko, but Celery includes many more primitives for the distribution of tasks and the collection of results.

Zato
----

`Zato <http://zato.io>`_ is a full `Enterprise Service Bus <http://en.wikipedia.org/wiki/Enterprise_service_bus>`_ (ESB) and application server written in Python. It concentrates on joining lots of different services together, including APIs and a GUI for configuration. It also includes tools for load-balancing and deployment.

ESBs are often used as middleware between legacy services. You can write new Python services in Zato but they are structured quite differently and its scope is significantly larger than that of Nameko. See Martin Fowler's paper on `microservices <http://martinfowler.com/articles/microservices.html#MicroservicesAndSoa>`_ for a comparison to ESBs.

Kombu
-----

`Kombu <http://kombu.readthedocs.io/>`_ is a Python messaging library, used by both Celery and Nameko. It exposes a high-level interface for AMQP and includes support for "virtual" transports, so can be run with non-AMQP transports such as Redis, ZeroMQ and MongoDB.

Nameko's AMQP features are built using Kombu but don't include support for the "virtual" transports.

Also, due to the usage of `eventlet <http://eventlet.net/>`_ for green concurrency, Nameko can't make use of C-extensions such as `librabbitmq <https://pypi.python.org/pypi/librabbitmq>`_ that Kombu uses by default if it's available. If you want to have `librabbitmq <https://pypi.python.org/pypi/librabbitmq>`_ in your environment for other purposes than Nameko, you can force Kombu to use standard Python implementation of AMQP by defining broker urls as ``pyamqp://`` instead of ``amqp://``

Eventlet
--------

`Eventlet <http://eventlet.net/>`_ is a Python library that provides concurrency via "greenthreads". You can check more details on how it's used by Nameko in the :ref:`Concurrency <concurrency>` section.
