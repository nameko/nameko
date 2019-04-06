'''
Provides core messaging decorators and dependency providers.
'''
from __future__ import absolute_import

import warnings
from functools import partial
from logging import getLogger

import six
from amqp.exceptions import ConnectionError
from eventlet.event import Event
from kombu import Connection
from kombu.common import maybe_declare
from kombu.mixins import ConsumerMixin

from nameko.amqp.publish import Publisher as PublisherCore
from nameko.amqp.publish import get_connection
from nameko.constants import (
    AMQP_SSL_CONFIG_KEY, AMQP_URI_CONFIG_KEY, DEFAULT_HEARTBEAT,
    DEFAULT_TRANSPORT_OPTIONS, HEADER_PREFIX, HEARTBEAT_CONFIG_KEY,
    TRANSPORT_OPTIONS_CONFIG_KEY
)
from nameko.exceptions import ContainerBeingKilled
from nameko.extensions import (
    DependencyProvider, Entrypoint, ProviderCollector, SharedExtension
)
from nameko.utils import sanitize_url


_log = getLogger(__name__)


class HeaderEncoder(object):

    header_prefix = HEADER_PREFIX

    def _get_header_name(self, key):
        return "{}.{}".format(self.header_prefix, key)

    def get_message_headers(self, worker_ctx):
        data = worker_ctx.context_data

        if None in data.values():
            warnings.warn(
                'Attempted to publish unserialisable header value. '
                'Headers with a value of `None` will be dropped from '
                'the payload.', UserWarning)

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

    def unpack_message_headers(self, message):
        stripped = {
            self._strip_header_name(k): v
            for k, v in six.iteritems(message.headers)
        }
        return stripped


class Publisher(DependencyProvider, HeaderEncoder):

    publisher_cls = PublisherCore

    def __init__(self, exchange=None, queue=None, declare=None, **options):
        """ Provides an AMQP message publisher method via dependency injection.

        In AMQP, messages are published to *exchanges* and routed to bound
        *queues*. This dependency accepts the `exchange` to publish to and
        will ensure that it is declared before publishing.

        Optionally, you may use the `declare` keyword argument to pass a list
        of other :class:`kombu.Exchange` or :class:`kombu.Queue` objects to
        declare before publishing.

        :Parameters:
            exchange : :class:`kombu.Exchange`
                Destination exchange
            queue : :class:`kombu.Queue`
                **Deprecated**: Bound queue. The event will be published to
                this queue's exchange.
            declare : list
                List of :class:`kombu.Exchange` or :class:`kombu.Queue` objects
                to declare before publishing.

        If `exchange` is not provided, the message will be published to the
        default exchange.

        Example::

            class Foobar(object):

                publish = Publisher(exchange=...)

                def spam(self, data):
                    self.publish('spam:' + data)
        """
        self.exchange = exchange
        self.options = options

        self.declare = declare[:] if declare is not None else []

        if self.exchange:
            self.declare.append(self.exchange)

        if queue is not None:
            warnings.warn(
                "The signature of `Publisher` has changed. The `queue` kwarg "
                "is now deprecated. You can use the `declare` kwarg "
                "to provide a list of Kombu queues to be declared. "
                "See CHANGES, version 2.7.0 for more details. This warning "
                "will be removed in version 2.9.0.",
                DeprecationWarning
            )
            if exchange is None:
                self.exchange = queue.exchange
            self.declare.append(queue)

        # backwards compat
        compat_attrs = ('retry', 'retry_policy', 'use_confirms')

        for compat_attr in compat_attrs:
            if hasattr(self, compat_attr):
                warnings.warn(
                    "'{}' should be specified at instantiation time rather "
                    "than as a class attribute. See CHANGES, version 2.7.0 "
                    "for more details. This warning will be removed in "
                    "version 2.9.0.".format(compat_attr), DeprecationWarning
                )
                self.options[compat_attr] = getattr(self, compat_attr)

    @property
    def amqp_uri(self):
        return self.container.config[AMQP_URI_CONFIG_KEY]

    @property
    def serializer(self):
        """ Default serializer to use when publishing messages.

        Must be registered as a
        `kombu serializer <http://bit.do/kombu_serialization>`_.
        """
        return self.container.serializer

    def setup(self):

        ssl = self.container.config.get(AMQP_SSL_CONFIG_KEY)

        with get_connection(self.amqp_uri, ssl) as conn:
            for entity in self.declare:
                maybe_declare(entity, conn.channel())

        serializer = self.options.pop('serializer', self.serializer)

        self.publisher = self.publisher_cls(
            self.amqp_uri,
            serializer=serializer,
            exchange=self.exchange,
            declare=self.declare,
            ssl=ssl,
            **self.options
        )

    def get_dependency(self, worker_ctx):
        extra_headers = self.get_message_headers(worker_ctx)

        def publish(msg, **kwargs):
            self.publisher.publish(
                msg, extra_headers=extra_headers, **kwargs
            )

        return publish


class QueueConsumer(SharedExtension, ProviderCollector, ConsumerMixin):

    def __init__(self):

        self._consumers = {}
        self._pending_remove_providers = {}

        self._gt = None
        self._starting = False

        self._consumers_ready = Event()
        super(QueueConsumer, self).__init__()

    @property
    def amqp_uri(self):
        return self.container.config[AMQP_URI_CONFIG_KEY]

    @property
    def prefetch_count(self):
        return self.container.max_workers

    @property
    def accept(self):
        return self.container.accept

    def _handle_thread_exited(self, gt):
        exc = None
        try:
            gt.wait()
        except Exception as e:
            exc = e

        if not self._consumers_ready.ready():
            self._consumers_ready.send_exception(exc)

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
        # only attempt to ack if the message connection is alive;
        # otherwise the message will already have been reclaimed by the broker
        if message.channel.connection:
            try:
                message.ack()
            except ConnectionError:  # pragma: no cover
                pass  # ignore connection closing inside conditional

    def requeue_message(self, message):
        # only attempt to requeue if the message connection is alive;
        # otherwise the message will already have been reclaimed by the broker
        if message.channel.connection:
            try:
                message.requeue()
            except ConnectionError:  # pragma: no cover
                pass  # ignore connection closing inside conditional

    def _cancel_consumers_if_requested(self):
        provider_remove_events = self._pending_remove_providers.items()
        self._pending_remove_providers = {}

        for provider, removed_event in provider_remove_events:
            consumer = self._consumers.pop(provider)

            _log.debug('cancelling consumer [%s]: %s', provider, consumer)
            consumer.cancel()
            removed_event.send()

    @property
    def connection(self):
        """ Provide the connection parameters for kombu's ConsumerMixin.

        The `Connection` object is a declaration of connection parameters
        that is lazily evaluated. It doesn't represent an established
        connection to the broker at this point.
        """
        heartbeat = self.container.config.get(
            HEARTBEAT_CONFIG_KEY, DEFAULT_HEARTBEAT
        )
        transport_options = self.container.config.get(
            TRANSPORT_OPTIONS_CONFIG_KEY, DEFAULT_TRANSPORT_OPTIONS
        )
        ssl = self.container.config.get(AMQP_SSL_CONFIG_KEY)
        conn = Connection(self.amqp_uri,
                          transport_options=transport_options,
                          heartbeat=heartbeat,
                          ssl=ssl
                          )

        return conn

    def handle_message(self, provider, body, message):
        ident = u"{}.handle_message[{}]".format(
            type(provider).__name__, message.delivery_info['routing_key']
        )
        self.container.spawn_managed_thread(
            partial(provider.handle_message, body, message), identifier=ident
        )

    def get_consumers(self, consumer_cls, channel):
        """ Kombu callback to set up consumers.

        Called after any (re)connection to the broker.
        """
        _log.debug('setting up consumers %s', self)

        for provider in self._providers:
            callbacks = [partial(self.handle_message, provider)]

            consumer = consumer_cls(
                queues=[provider.queue],
                callbacks=callbacks,
                accept=self.accept
            )
            consumer.qos(prefetch_count=self.prefetch_count)

            self._consumers[provider] = consumer

        return self._consumers.values()

    def on_iteration(self):
        """ Kombu callback for each `drain_events` loop iteration."""
        self._cancel_consumers_if_requested()

        if len(self._consumers) == 0:
            _log.debug('requesting stop after iteration')
            self.should_stop = True

    def on_connection_error(self, exc, interval):
        _log.warning(
            "Error connecting to broker at {} ({}).\n"
            "Retrying in {} seconds."
            .format(sanitize_url(self.amqp_uri), exc, interval))

    def on_consume_ready(self, connection, channel, consumers, **kwargs):
        """ Kombu callback when consumers are ready to accept messages.

        Called after any (re)connection to the broker.
        """
        if not self._consumers_ready.ready():
            _log.debug('consumer started %s', self)
            self._consumers_ready.send(None)


class Consumer(Entrypoint, HeaderDecoder):

    queue_consumer = QueueConsumer()

    def __init__(self, queue, requeue_on_error=False, **kwargs):
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
        super(Consumer, self).__init__(**kwargs)

    def setup(self):
        self.queue_consumer.register_provider(self)

    def stop(self):
        self.queue_consumer.unregister_provider(self)

    def handle_message(self, body, message):
        args = (body,)
        kwargs = {}

        context_data = self.unpack_message_headers(message)

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
