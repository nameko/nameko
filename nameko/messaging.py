'''
Provides core messaging decorators and dependency injection providers.
'''
from __future__ import absolute_import
from itertools import count
from functools import partial
from logging import getLogger
import socket

import eventlet
from eventlet.event import Event

from kombu.common import maybe_declare
from kombu.pools import producers, connections
from kombu import Connection
from kombu.mixins import ConsumerMixin

from nameko.dependencies import (
    InjectionProvider, EntrypointProvider, entrypoint, injection,
    DependencyProvider, ProviderCollector, DependencyFactory, dependency,
    CONTAINER_SHARED)

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


@injection
def publisher(exchange=None, queue=None):
    return DependencyFactory(PublishProvider, exchange, queue)


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


@dependency
def queue_consumer():
    return DependencyFactory(QueueConsumer)


class QueueConsumer(DependencyProvider, ProviderCollector, ConsumerMixin):
    def __init__(self):
        super(QueueConsumer, self).__init__()
        self._connection = None

        self._consumers = {}

        self._pending_messages = set()
        self._pending_ack_messages = []
        self._pending_requeue_messages = []
        self._pending_remove_providers = {}

        self._gt = None
        self._starting = False

        self._consumers_ready = Event()

    @property
    def _amqp_uri(self):
        return self.container.config[AMQP_URI_CONFIG_KEY]

    @property
    def _prefetch_count(self):
        return self.container.max_workers

    def start(self):
        if not self._starting:
            self._starting = True

            _log.debug('starting %s', self)
            self._gt = self.container.spawn_managed_thread(self.run)
        try:
            _log.debug('waiting for consumer ready %s', self)
            self._consumers_ready.wait()
        except QueueConsumerStopped:
            _log.debug('consumer was stopped before it started %s', self)
        else:
            _log.debug('started %s', self)

    def stop(self):
        """ Stop the queue-consumer gracefully.

        Wait until the last provider has been unregistered and for
        the ConsumerMixin's greenthread to exit (i.e. until all pending
        messages have been acked or requeued and all consumers stopped).
        """
        if not self._consumers_ready.ready():
            _log.debug('stopping while consumer is starting %s', self)

            stop_exc = QueueConsumerStopped()

            # stopping before we have started successfully by brutally
            # killing the consumer thread as we don't have a way to hook
            # into the pre-consumption startup process
            self._gt.kill(stop_exc)
            # we also want to let the start method know that we died.
            # it is waiting for the consumer to be ready
            # so we send the same exceptions
            self._consumers_ready.send_exception(stop_exc)

        _log.debug('waiting for providers to unregister %s', self)
        self._last_provider_unregistered.wait()
        _log.debug('all providers unregistered %s', self)

        try:
            _log.debug('waiting for consumer death %s', self)
            self._gt.wait()
        except QueueConsumerStopped:
            pass

        super(QueueConsumer, self).stop()
        _log.debug('stopped %s', self)

    def kill(self, exc):
        """ Kill the queue-consumer.

        Unlike `stop()` any pending message ack or requeue-requests,
        requests to remove providers, etc are lost and the consume thread is
        asked to terminate as soon as possible.
        """
        # greenlet has a magic attribute ``dead`` - pylint: disable=E1101
        if not self._gt.dead:
            # we can't just kill the thread because we have to give
            # ConsumerMixin a chance to close the sockets properly.
            self._providers = set()
            self._pending_messages = set()
            self._pending_ack_messages = []
            self._pending_requeue_messages = []
            self._pending_remove_providers = {}
            self.should_stop = True
            self._gt.wait()

            super(QueueConsumer, self).kill(exc)
            _log.debug('killed %s', self)

    def unregister_provider(self, provider):
        if not self._consumers_ready.ready():
            # we cannot handle the situation where we are starting up and
            # want to remove a consumer at the same time
            # TODO: With the upcomming error handling mechanism, this needs
            # TODO: to be thought through again.
            self.last_provider_unregistered()
            return

        removed_event = Event()
        # we can only cancel a consumer from within the consumer thread
        self._pending_remove_providers[provider] = removed_event
        # so we will just register the consumer to be cancleed
        removed_event.wait()

        super(QueueConsumer, self).unregister_provider(provider)

    def ack_message(self, message):
        _log.debug("stashing message-ack: %s", message)
        self._pending_messages.remove(message)
        self._pending_ack_messages.append(message)

    def requeue_message(self, message):
        _log.debug("stashing message-requeue: %s", message)
        self._pending_messages.remove(message)
        self._pending_requeue_messages.append(message)

    def _on_message(self, body, message):
        _log.debug("received message: %s", message)
        self._pending_messages.add(message)

    def _cancel_consumers_if_requested(self):
        provider_remove_events = self._pending_remove_providers.items()
        self._pending_remove_providers = {}

        for provider, removed_event in provider_remove_events:
            consumer = self._consumers.pop(provider)

            _log.debug('cancelling consumer [%s]: %s', provider, consumer)
            consumer.cancel()
            removed_event.send()

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

        for provider in self._providers:
            callbacks = [self._on_message, provider.handle_message]

            consumer = Consumer(queues=[provider.queue], callbacks=callbacks)
            consumer.qos(prefetch_count=self._prefetch_count)

            self._consumers[provider] = consumer

        return self._consumers.values()

    def on_iteration(self):
        """ kombu callback for each drain_events loop iteration."""
        self._cancel_consumers_if_requested()

        self._process_pending_message_acks()

        num_consumers = len(self._consumers)
        num_pending_messages = len(self._pending_messages)

        if num_consumers + num_pending_messages == 0:
            _log.debug('requesting stop after iteration')
            self.should_stop = True

    def on_consume_ready(self, connection, channel, consumers, **kwargs):
        """ kombu callback when consumers have been set up and before the
        first message is consumed """

        _log.debug('consumer started')
        self._consumers_ready.send(None)

    def consume(self, limit=None, timeout=None, safety_interval=0.1, **kwargs):
        elapsed = 0
        with self.consumer_context(**kwargs) as (conn, channel, consumers):
            for i in limit and range(limit) or count():
                # moved from after the following `should_stop` condition to
                # avoid waiting on a drain_events timeout before breaking
                # the loop.
                self.on_iteration()

                if self.should_stop:
                    break

                try:
                    conn.drain_events(timeout=safety_interval)
                except socket.timeout:
                    elapsed += safety_interval
                    # Excluding the following clause from coverage,
                    # as timeout never appears to be set - This method
                    # is a lift from kombu so will leave in place for now.
                    if timeout and elapsed >= timeout:  # pragma: no cover
                        raise
                except socket.error:
                    if not self.should_stop:
                        raise
                else:
                    yield
                    elapsed = 0


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


# pylint: disable=E1101,E1123
class ConsumeProvider(EntrypointProvider, HeaderDecoder):

    queue_consumer = queue_consumer(shared=CONTAINER_SHARED)

    def __init__(self, queue, requeue_on_error):
        self.queue = queue
        self.requeue_on_error = requeue_on_error

    def prepare(self):
        self.queue_consumer.register_provider(self)

    def stop(self):
        self.queue_consumer.unregister_provider(self)

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

        if exc is not None and self.requeue_on_error:
            self.queue_consumer.requeue_message(message)
        else:
            self.queue_consumer.ack_message(message)


class QueueConsumerStopped(Exception):
    pass
