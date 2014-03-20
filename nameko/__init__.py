"""
Getting Started
---------------

Nameko makes writing services easier by taking care of lifecycle management
and interface plumbing. Just declare your dependencies, specify a configuration
and write your business logic.

Let's create a simple example.

Hello World
===========

We'll start with service that prints "Hello World" when it receives an event.
Here's the business logic:

.. code:: python

    class HelloWorld(object):

        def hello(self, name):
            print "Hello, {}!".format(name)

To make it respond to an event, we have to make the ``hello`` method an
event handler. We do that by declaring it with the ``event_handler``
decorator:

.. code:: python

  from nameko.events import event_handler

  class HelloWorld(object):

      @event_handler('friendlyservice', 'hello')
      def hello(self, name):
          print "Hello, {}!".format(name)

Now, whenever a "friendlyservice" fires a "hello" event, ``hello`` will be
called. Let's create ``FriendlyService`` now:

.. code:: python

    from nameko.events import Event, event_dispatcher
    from nameko.timer import timer

    class HelloEvent(Event):
        type = "hello"

    class FriendlyService(object):

        name = "friendlyservice"
        dispatch = event_dispatcher()

        @timer(interval=5)
        def say_hello(self):
            self.dispatch(HelloEvent(self.name))

FriendlyService declares itself to be an event dispatcher with the
``event_dispatcher`` function. This gives it the ability to send events using
its ``dispatch`` method.

Using the nameko ``timer``, another declaration, instances of
``FriendlyService`` dispatch a hello event every 5 seconds. Let's use nameko
to make these two services interact:

.. code:: python

    from nameko.runners import ServiceRunner

    config = {'AMQP_URI': 'amqp://guest:guest@localhost:5672/'}
    runner = ServiceRunner(config)
    runner.add_service(HelloWorld)
    runner.add_service(FriendlyService)
    runner.start()
    try:
        runner.wait()
    except KeyboardInterrupt:
        runner.kill()

You will see "Hello, friendlyservice!" printed in the terminal every five
seconds. You will also find an exchange called ``friendlyservice.events``
in your AMQP broker and see messages passing through it.

The complete source for this example is available here:
:ref:`hello-world-example`.

.. note::

   ``@event_handler`` and ``@timer`` are examples of *entrypoint dependencies*.
   That is, they declare things on which the service *depends* to function as
   intended, and they provide an *entrypoint* into the business logic of the
   service. When they're triggered, some logic created by the service
   developer is invoked.

   ``event_dispatcher`` is an example of an *injection dependency*. It declares
   a dependency of the service that will be *injected* into the service
   instance when it is hosted by nameko.

RPC Example
===========

Nameko comes with RPC support out of the box. To expose a method over RPC,
decorate it with the ``@rpc`` entrypoint.

Adder service:

.. code:: python

    from nameko.rpc import rpc

    class AdderService(object):

        @rpc
        def add(self, x, y):
            return x + y

If your service needs to call an RPC method in another service, you can use
the ``rpc_proxy`` injection to access it.

Adder client:

.. code:: python

    import random

    from nameko.rpc import rpc, rpc_proxy
    from nameko.timer import timer


    class RpcClient(object):

        adder = rpc_proxy('adderservice')

        @timer(interval=2)
        def add(self):
            x = random.randint(0, 10)
            y = random.randint(0, 10)
            res = self.adder.add(x, y)
            print "{} + {} = {}".format(x, y, res)

The complete source for this example is available here:
:ref:`rpc-example`.

Messaging Example
=================

Underlying the RPC and Events features shown above is the lower-level
"messaging" codebase. You can use the messaging API to publish to and
consume from AMQP directly.

.. note::

   The messaging API is provided for low-level interaction with AMQP,
   usually when messages originate outside nameko. In the majority of cases
   it's preferable to use the events API.

.. code:: python

    demo_ex = Exchange('demo_ex', durable=False, auto_delete=True)
    demo_queue = Queue('demo_queue', exchange=demo_ex, durable=False,
                       auto_delete=True)

    class MessagingPublisher(object):
        "Publish messages to the ``demo_ex`` exchange every two seconds."
        publish = publisher(exchange=demo_ex)

        @timer(interval=2)
        def send_msg(self):
            msg = "log-{}".format(uuid.uuid4())
            self.publish(msg)

    class MessagingConsumer(object):
        "Consume messages from a queue bound to the ``demo_ex`` exchange."
        @consume(demo_queue)
        def process(self, payload):
            print payload

The source for this example, expanded with a custom injection dependency,
is available here: :ref:`messaging-example`.


API - reference
---------------

.. toctree::
    :maxdepth: 2

    Events (high level pub/sub)          <nameko.events>
    RPC over AMQP                        <nameko.rpc>
    Messaging (low level AMQP messaging) <nameko.messaging>
    Server                               <nameko.runners>
    Lifecycle Management                 <nameko.containers>
    Dependency Injection                 <nameko.dependencies>


Examples
--------

**Services**

.. toctree::
    :maxdepth: 2

    examples/index

**Testing**

The examples below demonstrate techniques and best practices for testing
services written with nameko.

The tests are intended to be run with `pytest <http://pytest.org/>`_. A
minimal :ref:`conftest` is included in the same directory as the test files.

.. toctree::
    :maxdepth: 2

    examples/testing/index

"""
