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


class Publisher(DependencyProvider):
    '''
    Provides a message publisher method via dependency injection.

    Publishers usually publish messages to an exchange which dispatches
    messages to bound queue.
    To simplify this various use cases a Publisher either accepts
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

    def get_instance(self, connection):
        '''
        Provides a publish method to a container.

        Args:
            connection: A kombu.Connection object.

        Returns:
            A publish(msg) method.
            The method, when called, will publish to the exchange defined
            during the initialization of the Publisher object.
        '''
        exchange = self.exchange
        queue = self.queue

        if exchange is None and queue is not None:
            exchange = queue.exchange

        def do_publish(msg):
            # TODO: would it not be better to to use a single connection
            #       per service, i.e. share it with consumers, etc?
            #       How will this work properly with eventlet?
            with producers[connection].acquire(block=True) as producer:
                channel = producer.channel
                if queue is not None:
                    maybe_declare(queue, channel)

                elif exchange is not None:
                    maybe_declare(exchange, channel)

                # TODO: should we enable auto-retry,
                #       should that be an option in __init__?
                producer.publish(msg, exchange=exchange)

        return do_publish


def consume(queue, fn=None):
    '''
    Decorates a method as a message consumer.

    Messaages from the queue will be deserialized depending on their content
    type and passed to the the decorated method.
    When the conumer method returns without raising any exceptions, the message
    will automatically be acknowledged.
    If any exceptions are raised during the consumtion, the message will be
    requeued.

    It is possible to manually acknowledge a message from within the consumer.
    To achieve this, it needs to accept a second parameter, the raw message
    object and call ack() or requeue() on it.

    Example::

        @consume(...)
        def handle_message(self, body, message):

            if not self.spam(body):
                message.requeue()

            self.shrub(body)

    Args:
        queue: The queue to consume from.
    '''
    if fn is None:
        return partial(consume, queue)

    argspecs = inspect.getargspec(fn)
    include_raw_message = len(argspecs.args) > 2
    consumer_configs[fn] = ConsumerConfig(queue, include_raw_message)

    return fn


class ConsumerConfig(object):
    '''
    Stores information about a consumer-decorated method.
    '''
    def __init__(self, queue, include_raw_message):
        self.queue = queue
        self.include_raw_message = include_raw_message
        self.prefetch_count = 1


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
    for name, consumer_method in inspect.getmembers(service, inspect.ismethod):
        try:
            consumer_config = consumer_configs[consumer_method.im_func]

            consumer = Consumer(
                            queues=[consumer_config.queue],
                            callbacks=[partial(on_message,
                                                consumer_config,
                                                consumer_method)]
                        )

            consumer.qos(prefetch_count=consumer_config.prefetch_count)

            yield consumer
        except KeyError:
            pass


def process_message(consumer_config, consumer, body, message):
    '''
    Processes a consumable message.

    If the consumer returns without raising any exceptions, the message
    will be acknowledged otherwise it will be requeued.

    The consumer may accept just the body or the body and the raw message.
    It may also ack() or requeue() the raw message manually, in which case
    automatic acknowledgement/requeueing is disabled.

    Args:
        consumer_config: The configuration as defined by the consume decorator.
        consumer: The consume-decorated method.

        body: The body of the message.
        message: The raw message.
    '''
    try:
        if consumer_config.include_raw_message:
            consumer(body, message)
        else:
            consumer(body)

    except Exception as e:
        if message.acknowledged:
            log.error('failed to consume message, '
                'cannot requeue because message already acknowledged: %s(): %s',
                consumer, e)
        else:
            log.error(
                    'failed to consume message, requeueing message: %s(): %s',
                    consumer, e)
            message.requeue()
    else:
        if not message.acknowledged:
            message.ack()

