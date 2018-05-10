'''
Provides core messaging decorators and dependency providers.
'''
from __future__ import absolute_import

import re
from functools import partial
from logging import getLogger

from kombu.common import maybe_declare

from nameko import serialization
from nameko.amqp.consume import Consumer as ConsumerCore
from nameko.amqp.publish import Publisher as PublisherCore
from nameko.amqp.publish import get_connection
from nameko.amqp.utils import verify_amqp_uri
from nameko.constants import (
    AMQP_URI_CONFIG_KEY, DEFAULT_HEARTBEAT, DEFAULT_PREFETCH_COUNT,
    HEARTBEAT_CONFIG_KEY, PREFETCH_COUNT_CONFIG_KEY, HEADER_PREFIX
)
from nameko.exceptions import ContainerBeingKilled
from nameko.extensions import DependencyProvider, Entrypoint


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
        return self.container.serializer

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


class Consumer(Entrypoint, HeaderDecoder):

    consumer_cls = ConsumerCore

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

    @property
    def amqp_uri(self):
        return self.container.config[AMQP_URI_CONFIG_KEY]

    def setup(self):
        verify_amqp_uri(self.amqp_uri)

        config = self.container.config

        heartbeat = config.get(HEARTBEAT_CONFIG_KEY, DEFAULT_HEARTBEAT)
        prefetch_count = config.get(
            PREFETCH_COUNT_CONFIG_KEY, DEFAULT_PREFETCH_COUNT
        )
        serializer, accept = serialization.setup(config)

        queues = [self.queue]
        callbacks = [self.handle_message]

        self.consumer = self.consumer_cls(
            self.amqp_uri, queues=queues, callbacks=callbacks,
            heartbeat=heartbeat, prefetch_count=prefetch_count,
            serializer=serializer, accept=accept
        )

    def start(self):
        self.consumer.should_stop = False
        self.container.spawn_managed_thread(self.consumer.run)
        self.consumer.wait_until_consumer_ready()

    def stop(self):
        self.consumer.should_stop = False

    def handle_message(self, body, message):
        args = (body,)
        kwargs = {}

        context_data = self.unpack_message_headers(message)

        handle_result = partial(self.handle_result, message)

        def spawn_worker():
            try:
                self.container.spawn_worker(
                    self, args, kwargs,
                    context_data=context_data,
                    handle_result=handle_result
                )
            except ContainerBeingKilled:
                self.requeue_message(message)

        service_name = self.container.service_name
        method_name = self.method_name

        # TODO replace global worker pool limits with per-entrypoint limits,
        # then remove this waiter thread
        ident = u"{}.wait_for_worker_pool[{}.{}]".format(
            type(self).__name__, service_name, method_name
        )
        self.container.spawn_managed_thread(spawn_worker, identifier=ident)

    def handle_result(self, message, worker_ctx, result=None, exc_info=None):
        self.handle_message_processed(message, result, exc_info)
        return result, exc_info

    def handle_message_processed(self, message, result=None, exc_info=None):

        if exc_info is not None and self.requeue_on_error:
            self.consumer.requeue_message(message)
        else:
            self.consumer.ack_message(message)


consume = Consumer.decorator
