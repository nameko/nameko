'''
Provides core messaging decorators and dependency injection providers.
'''
from __future__ import absolute_import
from functools import partial
from logging import getLogger
from weakref import WeakKeyDictionary
import inspect

from kombu.common import maybe_declare
from kombu.pools import producers

from nameko.dependencies import DependencyProvider

log = getLogger(__name__)

# stores the consumer configurations per method
consumer_configs = WeakKeyDictionary()

# delivery_mode
PERSISTENT = 2


class Publisher(DependencyProvider):
    '''
    Provides a message publisher method via dependency injection.

    Publishers usually push messages to an exchange, which dispatches
    them to bound queue.
    To simplify this for various use cases a Publisher either accepts
    a bound queue or an exchange and will ensure both are declared before
    a message is published.

    Example::

        class Foobar(object):

            publish = Publisher(exchange=...)

            def spam(self, data):
                self.publish('spam:' + data)

    '''

    def __init__(self, exchange=None, queue=None):
        self.exchange = exchange
        self.queue = queue

    def get_instance(self, container):
        '''
        Provides a publish method to a container.

        Args:
            container: A service container.

        Returns:
            A publish(msg) method.
            The method, when called, will publish to the exchange defined
            during the initialization of the Publisher object.
        '''
        exchange = self.exchange
        queue = self.queue

        if exchange is None and queue is not None:
            exchange = queue.exchange

        def do_publish(msg, **kwargs):
            # TODO: would it not be better to to use a single connection
            #       per service, i.e. share it with consumers, etc?
            #       How will this work properly with eventlet?
            with container.connection_factory() as conn:
                with producers[conn].acquire(block=True) as producer:
                    channel = producer.channel

                    if queue is not None:
                        maybe_declare(queue, channel)

                    elif exchange is not None:
                        maybe_declare(exchange, channel)

                    # TODO: should we enable auto-retry,
                    #       should that be an option in __init__?
                    producer.publish(msg, exchange=exchange, **kwargs)

        return do_publish


def consume(queue, requeue_on_error=False):
    '''
    Decorates a method as a message consumer.

    Messaages from the queue will be deserialized depending on their content
    type and passed to the the decorated method.
    When the conumer method returns without raising any exceptions,
    the message will automatically be acknowledged.
    If any exceptions are raised during the consumtion and
    `requeue_on_error` is True, the message will be requeued.

    Example::

        @consume(...)
        def handle_message(self, body):

            if not self.spam(body):
                raise Exception('message will be requeued')

            self.shrub(body)

    Args:
        queue: The queue to consume from.
    '''
    def consume_decorator(fn):
        consumer_configs[fn] = ConsumerConfig(queue, requeue_on_error)
        return fn

    return consume_decorator


class ConsumerConfig(object):
    '''
    Stores information about a consumer-decorated method.
    '''
    def __init__(self, queue, requeue_on_error):
        self.queue = queue
        self.requeue_on_error = requeue_on_error

    def get_queue(self, service, method_name):
        """ Base implementation for consumer config objects.
        ``service`` is provided for sub-classes if they need to create queues
        using information from the service object.

        Args:
            service - An instance of ``nameko.service.Service``.
        """
        return self.queue


def get_consumers(Consumer, service, on_message):
    '''
    Generates consumers for the consume-decorated method on a service.

    Args:
        Consumer: The Consumer class to use for a consumer.

        service: An object which may have consume-decorated methods.

    Returns:
        A generator with each item being a Consumer instance configured
        using the ConsumerConfig defined by the consume decorator.
    '''
    for name, consumer_method in inspect.getmembers(service.controller,
                                                    inspect.ismethod):
        try:
            consumer_config = consumer_configs[consumer_method.im_func]

            consumer = Consumer(
                queues=[consumer_config.get_queue(service, name)],
                callbacks=[
                    partial(on_message, (consumer_method, consumer_config))
                ]
            )
            yield consumer
        except KeyError:
            pass
