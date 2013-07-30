'''
Provides core messaging decorators and dependency injection providers.
'''
from __future__ import absolute_import
from itertools import count
from functools import partial
from logging import getLogger
import socket
from weakref import WeakKeyDictionary

import eventlet
from eventlet.event import Event

from kombu.common import maybe_declare
from kombu.pools import producers, connections
from kombu import Connection
from kombu.mixins import ConsumerMixin

from nameko.dependencies import (
    AttributeDependency, DecoratorDependency, dependency_decorator)

_log = getLogger(__name__)

# delivery_mode
PERSISTENT = 2


class Publisher(AttributeDependency):
    """
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

    """
    def __init__(self, exchange=None, queue=None):
        self.exchange = exchange
        self.queue = queue

    def get_connection(self, srv_ctx):
        conn = Connection(srv_ctx['config']['amqp_uri'])
        return connections[conn].acquire(block=True)

    def get_producer(self, srv_ctx):
        conn = Connection(srv_ctx['config']['amqp_uri'])
        return producers[conn].acquire(block=True)

    def start(self, srv_ctx):
        exchange = self.exchange
        queue = self.queue

        with self.get_connection(srv_ctx) as conn:
            if queue is not None:
                maybe_declare(queue, conn)
            elif exchange is not None:
                maybe_declare(exchange, conn)

    def __call__(self, srv_ctx, msg, **kwargs):
        """ Invoke this dependency's action.
        """
        exchange = self.exchange
        queue = self.queue

        if exchange is None and queue is not None:
            exchange = queue.exchange

        with self.get_producer(srv_ctx) as producer:
            # TODO: should we enable auto-retry,
            #       should that be an option in __init__?
            producer.publish(msg, exchange=exchange, **kwargs)


@dependency_decorator
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
    return ConsumeProvider(queue, requeue_on_error)


queue_consumers = WeakKeyDictionary()


def get_queue_consumer(srv_ctx):
    """ Get or create a QueueConsumer instance for the given ``srv_ctx``
    """
    container = srv_ctx['container']
    if container not in queue_consumers:
        queue_consumer = QueueConsumer(srv_ctx['config']['amqp_uri'])
        queue_consumers[container] = queue_consumer

    return queue_consumers[container]


class ConsumeProvider(DecoratorDependency):

    def __init__(self, queue, requeue_on_error):
        self.queue = queue
        self.requeue_on_error = requeue_on_error

    def start(self, srv_ctx):
        qc = get_queue_consumer(srv_ctx)
        qc.add_consumer(self.queue, partial(self.handle_message, srv_ctx))

    def on_container_started(self, srv_ctx):
        qc = get_queue_consumer(srv_ctx)
        qc.start()

    def stop(self, srv_ctx):
        qc = get_queue_consumer(srv_ctx)
        qc.stop()

    def handle_message(self, srv_ctx, body, message):
        callback = partial(self.handle_message_processed, message)
        args = (body,)
        kwargs = {}
        #TODO: if we have e.g. timers taking up all workers and a consumer
        # tries to spawn a next worker, what should the behavior be?
        # We can't block the the consumer thread as it would mean
        # current workers could not ack their messages and we'd deadlock

        srv_ctx['container'].spawn_worker(self.name, args, kwargs, callback)

    def handle_message_processed(self, message, worker_ctx):
        qc = get_queue_consumer(worker_ctx['srv_ctx'])

        if worker_ctx['data']['exc'] is not None and self.requeue_on_error:
            qc.requeue_message(message)
        else:
            qc.ack_message(message)


class QueueConsumer(ConsumerMixin):
    def __init__(self, amqp_uri):
        self._connection = None
        self._amqp_uri = amqp_uri
        self._registry = []

        self._pending_messages = set()
        self._pending_ack_messages = []
        self._pending_requeue_messages = []

        self._cancel_consumers = False
        self._consumers_stopped = False

        self._gt = None
        self._consumers_ready = Event()

    def start(self):
        if self._gt is None:
            _log.debug('starting')
            self._gt = eventlet.spawn(self.run)
            self._consumers_ready.wait()

    def stop(self):
        if self._gt is not None:
            _log.debug('stopping')
            self._cancel_consumers = True
            self._gt.wait()
            self._gt = None
            _log.debug('stopped')

    def add_consumer(self, queue, on_message):
        _log.debug("queueconsumer add_consumer {}, {}".format(queue,
                                                              on_message))
        self._registry.append((queue, on_message))

    def ack_message(self, message):
        _log.debug("queueconsumer ack_message {}".format(message))
        self._pending_messages.remove(message)
        self._pending_ack_messages.append(message)

    def requeue_message(self, message):
        self._pending_messages.remove(message)
        self._pending_requeue_messages.append(message)

    def _on_message(self, handle_message, body, message):
        _log.debug("queueconsumer _on_message {} {}".format(body, message))
        self._pending_messages.add(message)
        handle_message(body, message)

    def _cancel_consumers_if_requested(self):
        if self._cancel_consumers:
            if self._consumers:
                self._cancel_consumers = False
                _log.debug('cancelling consumers')

                for consumer in self._consumers:
                    consumer.cancel()

            self._consumers_stopped = True

    def _process_pending_message_acks(self):
        messages = self._pending_ack_messages
        if messages:
            _log.debug('ack() %d processed messages', len(messages))
            while messages:
                msg = messages.pop()
                msg.ack()
                eventlet.sleep()

        messages = self._pending_requeue_messages
        if messages:
            _log.debug('requeue() %d processed messages', len(messages))
            while messages:
                msg = messages.pop()
                msg.requeue()
                eventlet.sleep()

    @property
    def connection(self):
        """ kombu requirement """
        if self._connection is None:
            self._connection = Connection(self._amqp_uri)

        return self._connection

    def get_consumers(self, Consumer, channel):
        """ kombu callback to set up consumers """
        _log.debug('settting up consumers')

        consumers = []
        for queue, handle_message in self._registry:
            callback = partial(self._on_message, handle_message)
            consumer = Consumer(queues=[queue], callbacks=[callback])
            consumer.qos(prefetch_count=10)
            consumers.append(consumer)

        self._consumers = consumers
        return consumers

    def on_iteration(self):
        """ kombu callback for each drain_events loop iteration."""
        self._cancel_consumers_if_requested()

        self._process_pending_message_acks()

        if self._consumers_stopped and not self._pending_messages:
            _log.debug('requesting stop after iteration')
            self.should_stop = True

    def on_consume_ready(self, connection, channel, consumers, **kwargs):
        """ kombu callback when consumers have been set up and before the
        first message is consumed """

        _log.debug('consumer started')
        self._consumers_ready.send(None)

    def consume(self, limit=None, timeout=None, safety_interval=0.1, **kwargs):
        """ Lifted from kombu so we are able to break the loop immediately
            after a shutdown is triggered rather than waiting for the timeout.
        """
        elapsed = 0
        with self.Consumer() as (connection, channel, consumers):
            with self.extra_context(connection, channel):
                self.on_consume_ready(connection, channel, consumers, **kwargs)
                for i in limit and xrange(limit) or count():
                    # moved from after the following `should_stop` condition to
                    # avoid waiting on a drain_events timeout before breaking
                    # the loop.
                    self.on_iteration()
                    if self.should_stop:
                        break

                    try:
                        connection.drain_events(timeout=safety_interval)
                    except socket.timeout:
                        elapsed += safety_interval
                        # Excluding the following clause from coverage,
                        # as timeout never appears to be set - This method
                        # is a lift from kombu so will leave in place for now.
                        if timeout and elapsed >= timeout:  # pragma: no cover
                            raise socket.timeout()
                    except socket.error:
                        if not self.should_stop:
                            raise
                    else:
                        yield
                        elapsed = 0
