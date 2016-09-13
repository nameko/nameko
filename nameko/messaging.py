'''
Provides core messaging decorators and dependency providers.
'''
from __future__ import absolute_import

import socket
from functools import partial
from itertools import count
from logging import getLogger

import eventlet
import six
from eventlet.event import Event
from kombu import Connection
from kombu.common import maybe_declare
from kombu.mixins import ConsumerMixin
from kombu.pools import connections, producers

from nameko.amqp import verify_amqp_uri
from nameko.constants import (
    AMQP_URI_CONFIG_KEY, DEFAULT_RETRY_POLICY, DEFAULT_SERIALIZER,
    SERIALIZER_CONFIG_KEY)
from nameko.exceptions import ContainerBeingKilled
from nameko.extensions import (
    DependencyProvider, Entrypoint, ProviderCollector, SharedExtension)

_log = getLogger(__name__)

# delivery_mode
PERSISTENT = 2
HEADER_PREFIX = "nameko"


class HeaderEncoder(object):

    header_prefix = HEADER_PREFIX

    def _get_header_name(self, key):
        return "{}.{}".format(self.header_prefix, key)

    def get_message_headers(self, worker_ctx):
        data = worker_ctx.context_data

        if None in data.values():
            _log.warn(
                'Attempted to publish unserialisable header value. '
                'Headers with a value of `None` will be dropped from '
                'the payload. %s', data)

        headers = {self._get_header_name(key): value
                   for key, value in data.items()
                   if value is not None}
        return headers


class HeaderDecoder(object):

    header_prefix = HEADER_PREFIX

    def _strip_header_name(self, key):
        full_prefix = "{}.".format(self.header_prefix)
        if key.startswith(full_prefix):
            return key[len(full_prefix):]
        return key

    def unpack_message_headers(self, worker_ctx_cls, message):
        stripped = {
            self._strip_header_name(k): v
            for k, v in six.iteritems(message.headers)
        }
        return stripped


class Publisher(DependencyProvider, HeaderEncoder):

    amqp_uri = None
    serializer = None

    def __init__(self, exchange=None, queue=None):
        """ Provides an AMQP message publisher method via dependency injection.

        In AMQP messages are published to *exchanges* and routed to bound
        *queues*. This dependency accepts either an `exchange` or a bound
        `queue`, and will ensure both are declared before publishing.

        :Parameters:
            exchange : :class:`kombu.Exchange`
                Destination exchange
            queue : :class:`kombu.Queue`
                Bound queue. The event will be published to this queue's
                exchange.

        If neither `queue` nor `exchange` are provided, the message will be
        published to the default exchange.

        Example::

            class Foobar(object):

                publish = Publisher(exchange=...)

                def spam(self, data):
                    self.publish('spam:' + data)
        """
        self.exchange = exchange
        self.queue = queue

    # TODO: should be a module level function
    def get_connection(self):
        conn = Connection(self.amqp_uri)
        return connections[conn].acquire(block=True)

    # TODO: should be a module level function
    def get_producer(self):
        conn = Connection(self.amqp_uri)
        return producers[conn].acquire(block=True)

    def setup(self):

        self.amqp_uri = self.container.config[AMQP_URI_CONFIG_KEY]

        self.serializer = self.container.config.get(
            SERIALIZER_CONFIG_KEY, DEFAULT_SERIALIZER
        )

        exchange = self.exchange
        queue = self.queue

        verify_amqp_uri(self.amqp_uri)

        with self.get_connection() as conn:
            if queue is not None:
                maybe_declare(queue, conn)
            elif exchange is not None:
                maybe_declare(exchange, conn)

    def get_dependency(self, worker_ctx):
        def publish(msg, **kwargs):
            exchange = self.exchange
            queue = self.queue
            serializer = self.serializer

            if exchange is None and queue is not None:
                exchange = queue.exchange

            retry = kwargs.pop('retry', True)
            retry_policy = kwargs.pop('retry_policy', DEFAULT_RETRY_POLICY)

            with self.get_producer() as producer:
                headers = self.get_message_headers(worker_ctx)
                producer.publish(
                    msg, exchange=exchange, headers=headers,
                    serializer=serializer,
                    retry=retry, retry_policy=retry_policy, **kwargs)

        return publish


class QueueConsumer(SharedExtension, ProviderCollector, ConsumerMixin):

    amqp_uri = None
    prefetch_count = None

    def __init__(self):
        self._connection = None

        self._consumers = {}

        self._pending_messages = set()
        self._pending_ack_messages = []
        self._pending_requeue_messages = []
        self._pending_remove_providers = {}

        self._gt = None
        self._starting = False

        self._consumers_ready = Event()
        super(QueueConsumer, self).__init__()

    def _handle_thread_exited(self, gt):
        exc = None
        try:
            gt.wait()
        except Exception as e:
            exc = e

        if not self._consumers_ready.ready():
            self._consumers_ready.send_exception(exc)

    def setup(self):
        self.amqp_uri = self.container.config[AMQP_URI_CONFIG_KEY]
        self.accept = self.container.accept
        self.prefetch_count = self.container.max_workers
        verify_amqp_uri(self.amqp_uri)

    def start(self):
        if not self._starting:
            self._starting = True

            _log.debug('starting %s', self)
            self._gt = self.container.spawn_managed_thread(self.run)
            self._gt.link(self._handle_thread_exited)
        try:
            _log.debug('waiting for consumer ready %s', self)
            self._consumers_ready.wait()
        except QueueConsumerStopped:
            _log.debug('consumer was stopped before it started %s', self)
        except Exception as exc:
            _log.debug('consumer failed to start %s (%s)', self, exc)
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

        self.wait_for_providers()

        try:
            _log.debug('waiting for consumer death %s', self)
            self._gt.wait()
        except QueueConsumerStopped:
            pass

        super(QueueConsumer, self).stop()
        _log.debug('stopped %s', self)

    def kill(self):
        """ Kill the queue-consumer.

        Unlike `stop()` any pending message ack or requeue-requests,
        requests to remove providers, etc are lost and the consume thread is
        asked to terminate as soon as possible.
        """
        # greenlet has a magic attribute ``dead`` - pylint: disable=E1101
        if self._gt is not None and not self._gt.dead:
            # we can't just kill the thread because we have to give
            # ConsumerMixin a chance to close the sockets properly.
            self._providers = set()
            self._pending_messages = set()
            self._pending_ack_messages = []
            self._pending_requeue_messages = []
            self._pending_remove_providers = {}
            self.should_stop = True
            try:
                self._gt.wait()
            except Exception as exc:
                # discard the exception since we're already being killed
                _log.warn(
                    'QueueConsumer %s raised `%s` during kill', self, exc)

            super(QueueConsumer, self).kill()
            _log.debug('killed %s', self)

    def unregister_provider(self, provider):
        if not self._consumers_ready.ready():
            # we cannot handle the situation where we are starting up and
            # want to remove a consumer at the same time
            # TODO: With the upcomming error handling mechanism, this needs
            # TODO: to be thought through again.
            self._last_provider_unregistered.send()
            return

        removed_event = Event()
        # we can only cancel a consumer from within the consumer thread
        self._pending_remove_providers[provider] = removed_event
        # so we will just register the consumer to be canceled
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
        """ Kombu requirement """
        if self.amqp_uri is None:
            return   # don't cache a connection during introspection

        if self._connection is None:
            self._connection = Connection(self.amqp_uri)

        return self._connection

    def get_consumers(self, Consumer, channel):
        """ Kombu callback to set up consumers.

        Called after any (re)connection to the broker.
        """
        _log.debug('setting up consumers %s', self)

        for provider in self._providers:
            callbacks = [self._on_message, provider.handle_message]

            consumer = Consumer(queues=[provider.queue], callbacks=callbacks,
                                accept=self.accept)
            consumer.qos(prefetch_count=self.prefetch_count)

            self._consumers[provider] = consumer

        return self._consumers.values()

    def on_iteration(self):
        """ Kombu callback for each `drain_events` loop iteration."""
        self._cancel_consumers_if_requested()

        self._process_pending_message_acks()

        num_consumers = len(self._consumers)
        num_pending_messages = len(self._pending_messages)

        if num_consumers + num_pending_messages == 0:
            _log.debug('requesting stop after iteration')
            self.should_stop = True

    def on_connection_error(self, exc, interval):
        _log.warn(
            "Error connecting to broker at {} ({}).\n"
            "Retrying in {} seconds.".format(self.amqp_uri, exc, interval))

    def on_consume_ready(self, connection, channel, consumers, **kwargs):
        """ Kombu callback when consumers are ready to accept messages.

        Called after any (re)connection to the broker.
        """
        if not self._consumers_ready.ready():
            _log.debug('consumer started %s', self)
            self._consumers_ready.send(None)

        for provider in self._providers:
            try:
                callback = provider.on_consume_ready
            except AttributeError:
                pass
            else:
                callback()

    def consume(self, limit=None, timeout=None, safety_interval=0.1, **kwargs):
        """ Lifted from Kombu.

        We switch the order of the `break` and `self.on_iteration()` to
        avoid waiting on a drain_events timeout before breaking the loop.
        """
        elapsed = 0
        with self.consumer_context(**kwargs) as (conn, channel, consumers):
            for i in limit and range(limit) or count():
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


class Consumer(Entrypoint, HeaderDecoder):

    queue_consumer = QueueConsumer()

    def __init__(self, queue, requeue_on_error=False):
        """
        Decorates a method as a message consumer.

        Messages from the queue will be deserialized depending on their content
        type and passed to the the decorated method.
        When the consumer method returns without raising any exceptions,
        the message will automatically be acknowledged.
        If any exceptions are raised during the consumption and
        `requeue_on_error` is True, the message will be requeued.

        If `requeue_on_error` is true, handlers will return the event to the
        queue if an error occurs while handling it. Defaults to false.

        Example::

            @consume(...)
            def handle_message(self, body):

                if not self.spam(body):
                    raise Exception('message will be requeued')

                self.shrub(body)

        Args:
            queue: The queue to consume from.
        """
        self.queue = queue
        self.requeue_on_error = requeue_on_error

    def setup(self):
        self.queue_consumer.register_provider(self)

    def stop(self):
        self.queue_consumer.unregister_provider(self)

    def handle_message(self, body, message):
        args = (body,)
        kwargs = {}

        # TODO: get valid headers from config, not worker_ctx_cls
        worker_ctx_cls = self.container.worker_ctx_cls
        context_data = self.unpack_message_headers(worker_ctx_cls, message)

        handle_result = partial(self.handle_result, message)
        try:
            self.container.spawn_worker(self, args, kwargs,
                                        context_data=context_data,
                                        handle_result=handle_result)
        except ContainerBeingKilled:
            self.queue_consumer.requeue_message(message)

    def handle_result(self, message, worker_ctx, result=None, exc_info=None):
        self.handle_message_processed(message, result, exc_info)
        return result, exc_info

    def handle_message_processed(self, message, result=None, exc_info=None):

        if exc_info is not None and self.requeue_on_error:
            self.queue_consumer.requeue_message(message)
        else:
            self.queue_consumer.ack_message(message)


consume = Consumer.decorator


class QueueConsumerStopped(Exception):
    pass
