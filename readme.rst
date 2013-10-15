Nameko
######

.. image:: https://secure.travis-ci.org/onefinestay/nameko.png?branch=master
   :target: http://travis-ci.org/onefinestay/nameko

**Nameko** ``[nah-meh-koh]`` (noun)

#. amber-brown mushroom with a slightly gelatinous coating that is used as an
   ingredient in miso soup and nabemono.
#. python services framework on top of AMQP

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

    from nameko.service import ServiceRunner
    
    config = {'AMQP_URI': 'amqp://guest:guest@localhost:5672/'}
    runner = ServiceRunner(config)
    runner.add_service(HelloWorld)
    runner.add_service(FriendlyService)
    runner.start()
    try:
        runner.wait()
    except KeyboardInterrupt:
        runner.kill()


RPC Example
===========

Nameko comes with RPC support out of the box. To expose a method over RPC,
decorate it with ``@rpc``.

Adder service:

.. code:: python

    from nameko.rpc import rpc

   class AdderService(object):
      
      @rpc
      def add(self, x, y):
         return x + y

If your service needs to call an RPC method in another service, you can use
the ``Service`` proxy to access it.

Adder client:

.. code:: python

   import random

   from nameko.rpc import rpc, Service
   from nameko.timer import timer
   
   
   class RpcClient(object):
   
      adder = Service('adderservice')
      
      @timer(interval=2)
      def add(self):
         x = random.randint(0, 10)
         y = random.randint(0, 10)
         res = self.adder.add(x, y)
         print "{} + {} = {}".format(x, y, res)

        
Messaging Example
=================

Underlying the RPC and Events features shown above is the lower-level
"messaging" codebase. You can use the messaging features to publish to and
consume from AMQP directly.

.. note::

   The messaging API is provided for low-level interaction with AMQP,
   usually when messages originate outside nameko. In the majority of cases
   it's preferable to use the events API.

.. code:: python

   demo_ex = Exchange('demo_ex', durable=False, auto_delete=True)
   demo_queue = Queue('demo_queue', exchange=demo_ex, durable=False,
                      auto_delete=True)

   class MessagingClient(object):
      """ Publish a message to the ``demo_ex`` exchange every two seconds.
      """
      publish = Publisher(exchange=demo_ex)
   
      @timer(interval=2)
      def send_msg(self):
         msg = "log-{}".format(uuid.uuid4())
         self.publish(msg)

   class ListenerService(object):
      """ Listen to messages a queue bound to the ``demo_ex`` exchange.
      """
      @consume(demo_queue)
      def process(self, payload):
         print payload


Dependencies
============

In the code snippets above, the ``timer``, ``consume`` and ``rpc`` decorators,
and the ``Publisher``, ``Service`` and ``EventDispatcher`` classes declare the
*dependencies* of their service.

Declaring dependencies is how a bare class becomes a nameko service, and the
dependencies are *injected* when the class is hosted.

Have a look at ``nameko.dependencies`` to see how nameko interfaces with
declared dependencies.


Writing Dependency Providers
============================

It's easy to write your own dependencies. Choose to extend either
``AttributeDependency`` or ``DecoratorDependency``, and implement the
appropriate interface methods.

Here's an example dependency that writes to a log file, making use of nameko's
lifecycle management to open, close and flush the file at apppropriate points.

.. code:: python

   class LogFile(AttributeDependency):
      
      # called at dependency creation time (i.e. service definition)
      def __init__(self, path):
         self.path = path
   
      # called when the service container starts
      def start(self, srv_ctx):
         self.file_handle = open(self.path, 'w')
   
      # called when the service container stops
      def stop(self, srv_ctx):
         self.file_handle.close()
   
      # called before this dependency's service handles any call
      def acquire_injection(self, worker_ctx):
         def log(msg):
            self.file_handle.write(msg + "\n")
         return log
   
      # called after this dependency's service handles a call
      def release_injection(self, worker_ctx):
         self.file_handle.flush()

Moving the 'plumbing' into a dependency means that service developers can
concentrate on the business logic of their code, and fosters a write-once,
use-many-times philosophy.

To incorporate this dependency into our ``ListenerService``, we'd do this:

.. code:: python

   class ListenerService(object):
      
      log = LogFile('/tmp/nameko')
   
      @consume(demo_queue)
      def process(self, payload):
         self.log(payload)

Working examples of the above can be found in docs/examples.


License
-------

Apache 2.0. See LICENSE for details.
