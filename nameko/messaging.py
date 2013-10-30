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
    InjectionProvider, EntrypointProvider, entrypoint, injection,
    DependencyFactory, DependencyProvider, dependency)

_log = getLogger(__name__)

# delivery_mode
PERSISTENT = 2
HEADER_PREFIX = "nameko"
AMQP_URI_CONFIG_KEY = 'AMQP_URI'


class HeaderEncoder(object):

    header_prefix = HEADER_PREFIX

    def get_message_headers(self, worker_ctx):
        headers = {}
        for key in worker_ctx.data_keys:
            if key in worker_ctx.data:
                name = "{}.{}".format(self.header_prefix, key)
                headers[name] = worker_ctx.data[key]
        return headers


class HeaderDecoder(object):

    header_prefix = HEADER_PREFIX

    def unpack_message_headers(self, worker_ctx_cls, message):
        data_keys = worker_ctx_cls.data_keys

        data = {}
        for key in data_keys:
            name = "{}.{}".format(self.header_prefix, key)
            if name in message.headers:
                data[key] = message.headers[name]
        return data


class PublishProvider(InjectionProvider, HeaderEncoder):
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

    def get_connection(self):
        # TODO: should this live outside of the class or be a class method?
        conn = Connection(self.container.config[AMQP_URI_CONFIG_KEY])
        return connections[conn].acquire(block=True)

    def get_producer(self):
        conn = Connection(self.container.config[AMQP_URI_CONFIG_KEY])
        return producers[conn].acquire(block=True)

    def prepare(self):
        exchange = self.exchange
        queue = self.queue

        with self.get_connection() as conn:
            if queue is not None:
                maybe_declare(queue, conn)
            elif exchange is not None:
                maybe_declare(exchange, conn)

    def acquire_injection(self, worker_ctx):
        def publish(msg, **kwargs):
            exchange = self.exchange
            queue = self.queue

            if exchange is None and queue is not None:
                exchange = queue.exchange

            with self.get_producer() as producer:
                # TODO: should we enable auto-retry,
                #      should that be an option in __init__?
                headers = self.get_message_headers(worker_ctx)
                producer.publish(msg, exchange=exchange, headers=headers,
                                 **kwargs)

        return publish


@injection
def publisher(exchange=None, queue=None):
    return DependencyFactory(PublishProvider, exchange, queue)


@entrypoint
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
    return DependencyFactory(ConsumeProvider, queue, requeue_on_error)


queue_consumers = WeakKeyDictionary()


def get_queue_consumer(container):
    """ Get or create a QueueConsumer instance for the given ServiceContainer
    instance.
    """
    if container not in queue_consumers:
        queue_consumer = QueueConsumer(
            container.config[AMQP_URI_CONFIG_KEY], container.max_workers)
        queue_consumers[container] = queue_consumer

    return queue_consumers[container]


class ConsumeProvider(EntrypointProvider, HeaderDecoder):

    def __init__(self, queue, requeue_on_error):
        self.queue = queue
        self.requeue_on_error = requeue_on_error

    def prepare(self):
        qc = get_queue_consumer(self.container)
        qc.add_consumer(self.queue, partial(self.handle_message))

    def start(self):
        qc = get_queue_consumer(self.container)
        qc.start()

    def stop(self):
        qc = get_queue_consumer(self.container)
        qc.stop()

    def kill(self, exc=None):
        qc = get_queue_consumer(self.container)
        qc.kill(exc)

    def handle_message(self, body, message):
        args = (body,)
        kwargs = {}

        worker_ctx_cls = self.container.worker_ctx_cls
        context_data = self.unpack_message_headers(worker_ctx_cls, message)

        self.container.spawn_worker(
            self, args, kwargs,
            context_data=context_data,
            handle_result=partial(self.handle_result, message))

    def handle_result(self, message, worker_ctx, result=None, exc=None):
        self.handle_message_processed(message, result, exc)

    def handle_message_processed(self, message, result=None, exc=None):

        qc = get_queue_consumer(self.container)

        if exc is not None and self.requeue_on_error:
            qc.requeue_message(message)
        else:
            qc.ack_message(message)


class QueueConsumerStopped(Exception):
    pass


class QueueConsumer(DependencyProvider, ConsumerMixin):
    def __init__(self):
        self._connection = None
        """
        self._amqp_uri = amqp_uri
        self._prefetch_count = prefetch_count
        """
        self._registry = []

        self._pending_messages = set()
        self._pending_ack_messages = []
        self._pending_requeue_messages = []

        self._cancel_consumers = False
        self._consumers_stopped = False

        self._gt = None
        self._starting = False
        self._stopping = False

        self._consumers_ready = Event()

    @property
    def _amqp_uri(self):
        return self.container.config[AMQP_URI_CONFIG_KEY]

    @property
    def _max_workers(self):
        return self.container.max_workers

    def start(self):
        if not self._starting:
            self._starting = True

            _log.debug('starting %s', self)
            self._gt = eventlet.spawn(self.run)
        else:
            _log.debug('already starting %s', self)

        try:
            _log.debug('waiting for consumer read %s', self)
            self._consumers_ready.wait()
        except QueueConsumerStopped:
            _log.debug('consumer was stopped before it started %s', self)
        else:
            _log.debug('started %s', self)

    def stop(self):
        if not self._stopping:
            self._stopping = True

            if not self._consumers_ready.ready():
                _log.debug('stopping while consumer is starting %s', self)

                stop_exc = QueueConsumerStopped()

                # stopping before we have started successfully by brutally
                # killing the consumer thread as we don't have a way to hook
                # into the pre-consumption startup process
                self.kill(stop_exc)
                # we also want to let the start method know that we died.
                # it is waiting for the consumer to be ready
                # so we send the same exceptions
                self._consumers_ready.send_exception(stop_exc)

            else:
                _log.debug('stopping %s', self)

                self._cancel_consumers = True
        else:
            _log.debug('already stopping %s', self)

        try:
            _log.debug('waiting for consumer death %s', self)
            self._gt.wait()
        except QueueConsumerStopped:
            pass

        _log.debug('stopped %s', self)

    def kill(self, exc):
        # greenlet has a magic attribute ``dead`` - pylint: disable=E1101
        if not self._gt.dead:
            self._gt.kill(exc)
            _log.debug('killed %s', self)

    def add_consumer(self, queue, on_message):
        _log.debug("adding consumer for %s, on_message: %s", queue, on_message)
        self._registry.append((queue, on_message))

    def ack_message(self, message):
        _log.debug("stashing message-ack: %s", message)
        self._pending_messages.remove(message)
        self._pending_ack_messages.append(message)

    def requeue_message(self, message):
        _log.debug("stashing message-requeue: %s", message)
        self._pending_messages.remove(message)
        self._pending_requeue_messages.append(message)

    def _on_message(self, handle_message, body, message):
        _log.debug("received message: %s", message)
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
            consumer.qos(prefetch_count=self._prefetch_count)
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
        with self.Consumer() as ccc:
            connection, channel, consumers = ccc
            with self.extra_context(connection, channel):
                self.on_consume_ready(connection, channel, consumers, **kwargs)
                for _ in limit and xrange(limit) or count():
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


@dependency
def queue_consumer():
    return DependencyFactory(QueueConsumer)
