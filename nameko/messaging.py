'''
Provides core messaging decorators and dependency providers.
'''
from __future__ import absolute_import

import re
from functools import partial
from logging import getLogger

from amqp.exceptions import ConnectionError
from eventlet.event import Event
from kombu import Connection
from kombu.common import maybe_declare
from kombu.mixins import ConsumerMixin

from nameko.amqp.publish import Publisher as PublisherCore
from nameko.amqp.publish import get_connection
from nameko.amqp.utils import verify_amqp_uri
from nameko.constants import (
    AMQP_URI_CONFIG_KEY, DEFAULT_HEARTBEAT, DEFAULT_SERIALIZER, HEADER_PREFIX,
    HEARTBEAT_CONFIG_KEY, SERIALIZER_CONFIG_KEY
)
from nameko.exceptions import ContainerBeingKilled
from nameko.extensions import (
    DependencyProvider, Entrypoint, ProviderCollector, SharedExtension
)


_log = getLogger(__name__)


def encode_to_headers(context_data, prefix=HEADER_PREFIX):
    return {
        "{}.{}".format(prefix, key): value
        for key, value in context_data.items()
        if value is not None
    }


def decode_from_headers(headers, prefix=HEADER_PREFIX):
    return {
        re.sub("^{}\.".format(prefix), "", key): value
        for key, value in headers.items()
    }


class HeaderEncoder(object):

    header_prefix = HEADER_PREFIX

    def get_message_headers(self, worker_ctx):
        data = worker_ctx.context_data

        if None in data.values():
            _log.warn(
                'Attempted to publish unserialisable header value. '
                'Headers with a value of `None` will be dropped from '
                'the payload. %s', data)

        return encode_to_headers(data, prefix=self.header_prefix)


class HeaderDecoder(object):

    header_prefix = HEADER_PREFIX

    def unpack_message_headers(self, message):
        return decode_from_headers(message.headers, prefix=self.header_prefix)


class Publisher(DependencyProvider, HeaderEncoder):

    publisher_cls = PublisherCore

    def __init__(self, exchange=None, declare=None, **options):
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

    @property
    def amqp_uri(self):
        return self.container.config[AMQP_URI_CONFIG_KEY]

    @property
    def serializer(self):
        """ Default serializer to use when publishing messages.

        Must be registered as a
        `kombu serializer <http://bit.do/kombu_serialization>`_.
        """
        return self.container.config.get(
            SERIALIZER_CONFIG_KEY, DEFAULT_SERIALIZER
        )

    def setup(self):

        verify_amqp_uri(self.amqp_uri)

        with get_connection(self.amqp_uri) as conn:
            for entity in self.declare:
                maybe_declare(entity, conn)

        serializer = self.options.pop('serializer', self.serializer)

        self.publisher = self.publisher_cls(
            self.amqp_uri,
            serializer=serializer,
            exchange=self.exchange,
            declare=self.declare,
            **self.options
        )

    def get_dependency(self, worker_ctx):
        extra_headers = self.get_message_headers(worker_ctx)

        def publish(msg, **kwargs):
            self.publisher.publish(
                msg, extra_headers=extra_headers, **kwargs
            )

        return publish


class Consumer(Entrypoint, HeaderDecoder, ConsumerMixin):

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
        self.consumer_ready = Event()
        super(Consumer, self).__init__(**kwargs)

    def setup(self):
        verify_amqp_uri(self.amqp_uri)
        with self.connection as conn:
            maybe_declare(self.queue, conn)

    def start(self):
        self.should_stop = False
        self.container.spawn_managed_thread(self.run)
        self.consumer_ready.wait()

    def stop(self):
        self.should_stop = False

    @property
    def amqp_uri(self):
        return self.container.config[AMQP_URI_CONFIG_KEY]

    @property
    def prefetch_count(self):
        return self.container.max_workers

    @property
    def serializer(self):
        return self.container.config.get(
            SERIALIZER_CONFIG_KEY, DEFAULT_SERIALIZER
        )

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
        return Connection(self.amqp_uri, heartbeat=heartbeat)

    def on_connection_error(self, exc, interval):
        _log.warning(
            "Error connecting to broker at {} ({}).\n"
            "Retrying in {} seconds.".format(self.amqp_uri, exc, interval)
        )

    def on_consume_ready(self, connection, channel, consumers, **kwargs):
        """ Kombu callback when consumers are ready to accept messages.

        Called after any (re)connection to the broker.
        """
        if not self.consumer_ready.ready():
            _log.debug('consumer started %s', self)
            self.consumer_ready.send(None)

    def get_consumers(self, consumer_cls, channel):
        """ Kombu callback to set up consumers.

        Called after any (re)connection to the broker.
        """
        consumer = consumer_cls(
            queues=[self.queue],
            callbacks=[self.handle_message],
            accept=[self.serializer]
        )
        consumer.qos(prefetch_count=self.prefetch_count)
        return [consumer]

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
            message.requeue()

    def handle_result(self, message, worker_ctx, result=None, exc_info=None):
        self.handle_message_processed(message, result, exc_info)
        return result, exc_info

    def handle_message_processed(self, message, result=None, exc_info=None):

        if exc_info is not None and self.requeue_on_error:
            self.requeue_message(message)
        else:
            self.ack_message(message)

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


consume = Consumer.decorator


class QueueConsumerStopped(Exception):
    pass
