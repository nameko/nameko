"""

Getting Started
---------------

Nameko makes writing services easier by taking care of lifecycle management
and interface plumbing. Just declare your dependencies, specify a configuration
and write your business logic.

Let's create a simple example.

We'll start with service that prints "Hello World" when it receives an event.
Here's the business logic:

.. code:: python

    class HelloWorld(object):

        def hello(self, name):
            print "Hello, {}!".format(name)

To make it respond to an event, we have to make ``hello`` an event handler:

.. code:: python

  from nameko.events import event_handler

  class HelloWorld(object):

      @event_handler('friendlyservice', 'hello')
      def hello(self, name):
          print "Hello, {}!".format(name)

Now, whenever a "friendlyservice" fires a "hello" event, ``hello`` will be
called. Let's create ``FriendlyService`` now:

.. code:: python

    from nameko.events import Event, EventDispatcher
    from nameko.timer import timer

    class HelloEvent(Event):
        type = "hello"

    class FriendlyService(object):

        name = "friendlyservice"
        dispatch = EventDispatcher()

        @timer(interval=5)
        def say_hello(self):
            self.dispatch(HelloEvent(self.name))


Using the nameko ``timer``,  instances of ``FriendlyService`` dispatch a hello
event every 5 seconds. Let's use nameko to make these two services interact:

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


API - reference
---------------

.. toctree::
    :maxdepth: 2

    Events (high level pub/sub) <nameko.events>
    RPC over AMQP <nameko.rpc>
    Messagng (low level AMQP messaging) <nameko.messaging>

    Server  <nameko.runners>

    Containers <nameko.containers>
    Dependency Injection <nameko.dependencies>


Examples
--------


"""
