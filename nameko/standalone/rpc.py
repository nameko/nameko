from __future__ import absolute_import

import logging
import socket
import uuid

from amqp.exceptions import NotFound
from kombu.common import maybe_declare
from kombu.messaging import Queue

from nameko import serialization
from nameko.amqp.consume import Consumer
from nameko.amqp.publish import Publisher, get_connection
from nameko.constants import (
    AMQP_SSL_CONFIG_KEY, AMQP_URI_CONFIG_KEY, DEFAULT_HEARTBEAT,
    DEFAULT_PREFETCH_COUNT, HEARTBEAT_CONFIG_KEY, PREFETCH_COUNT_CONFIG_KEY
)
from nameko.containers import new_call_id
from nameko.exceptions import ReplyQueueExpiredWithPendingReplies, RpcTimeout
from nameko.messaging import encode_to_headers
from nameko.rpc import (
    RESTRICTED_PUBLISHER_OPTIONS, RPC_REPLY_QUEUE_TEMPLATE,
    RPC_REPLY_QUEUE_TTL, Proxy, get_rpc_exchange
)


_logger = logging.getLogger(__name__)


class ReplyListener(object):
    """ Single-threaded listener for RPC replies.

    Creates a queue and consumes from it on demand. RPC requests
    should register their `correlation_id` with
    :meth:`~ReplyListener.register_for_reply` in order for the `ReplyListener`
    to capture the reply.
    """
    class ReplyConsumer(Consumer):
        """ Subclass Consumer to add disconnection check
        """

        def __init__(self, check_for_lost_replies, *args, **kwargs):
            self.check_for_lost_replies = check_for_lost_replies
            super(ReplyListener.ReplyConsumer, self).__init__(*args, **kwargs)

        def get_consumers(self, consumer_cls, channel):
            """
            Check for messages lost while the reply listener was disconnected
            from the broker.
            """
            self.check_for_lost_replies()

            return super(ReplyListener.ReplyConsumer, self).get_consumers(
                consumer_cls, channel
            )

    consumer_cls = ReplyConsumer

    def __init__(self, config, queue, timeout=None, **kwargs):
        self.config = config
        self.queue = queue
        self.timeout = timeout

        self.pending = {}
        super(ReplyListener, self).__init__(**kwargs)

    @property
    def amqp_uri(self):
        return self.config[AMQP_URI_CONFIG_KEY]

    def start(self):
        config = self.config

        ssl = config.get(AMQP_SSL_CONFIG_KEY)

        heartbeat = config.get(HEARTBEAT_CONFIG_KEY, DEFAULT_HEARTBEAT)
        prefetch_count = config.get(
            PREFETCH_COUNT_CONFIG_KEY, DEFAULT_PREFETCH_COUNT
        )
        accept = serialization.setup(config).accept

        queues = [self.queue]
        callbacks = [self.handle_message]

        self.consumer = self.consumer_cls(
            self.check_for_lost_replies, self.amqp_uri, ssl=ssl,
            queues=queues, callbacks=callbacks,
            heartbeat=heartbeat, prefetch_count=prefetch_count, accept=accept
        )

        # must declare queue because the consumer doesn't start right away
        with self.consumer.connection as conn:
            maybe_declare(self.queue, conn.channel())

    def stop(self):
        self.consumer.stop()

    def check_for_lost_replies(self):
        if self.pending:
            try:
                ssl = self.config.get(AMQP_SSL_CONFIG_KEY)
                with get_connection(self.amqp_uri, ssl=ssl) as conn:
                    self.queue.bind(conn).queue_declare(passive=True)
            except NotFound:
                raise ReplyQueueExpiredWithPendingReplies(
                    "Lost replies for correlation ids:\n{}".format(
                        "\n".join(self.pending.keys())
                    )
                )

    def register_for_reply(self, correlation_id):
        """ Register an RPC call with the given `correlation_id` for a reply.

        Returns a function that can be used to retrieve the reply, blocking
        until it has been received.
        """
        self.pending[correlation_id] = None
        return lambda: self.consume_reply(correlation_id)

    def consume_reply(self, correlation_id):
        """ Consume from the reply queue until the reply for the given
        `correlation_id` is received.
        """
        # return error if correlation_id not pending? (new feature)
        if self.consumer.should_stop:
            raise RuntimeError("Stopped and can no longer be used")

        while not self.pending.get(correlation_id):
            try:
                next(self.consumer.consume(timeout=self.timeout))
            except socket.timeout:
                raise RpcTimeout()
        return self.pending.pop(correlation_id)

    def handle_message(self, body, message):
        self.consumer.ack_message(message)

        correlation_id = message.properties.get('correlation_id')
        if correlation_id not in self.pending:
            _logger.debug("Unknown correlation id: %s", correlation_id)
            return

        self.pending[correlation_id] = body


class RpcProxy(object):
    """
    Single-threaded RPC proxy to a cluster of services. The target service
    and method are specified with attributes.

    Method calls on the local object are converted into RPC calls to the
    target service.

    Enables services not hosted by nameko to make RPC requests to a nameko
    cluster. It is commonly used as a context manager but may also be manually
    started and stopped.

    *Usage*

    As a context manager::

        with RpcProxy(config) as proxy:
            proxy.target_service.method()
            proxy.other_service.method()

    The equivalent call, manually starting and stopping::

        proxy = RpcProxy(config)
        proxy = proxy.start()
        try:
            proxy.target_service.method()
            proxy.other_service.method()
        finally:
            proxy.stop()

    If you call ``start()`` you must eventually call ``stop()`` to close the
    connection to the broker.

    You may also supply ``context_data``, a dictionary of data to be
    serialised into the AMQP message headers.

    When the name of the service is not legal in Python, you can also
    use a dict-like syntax::

        with RpcProxy(config) as proxy:
            proxy['service-name'].method()
            proxy['other-service'].method()

    """

    publisher_cls = Publisher

    def __init__(
        self, config, context_data=None, timeout=None, **publisher_options
    ):
        self.config = config
        self.uuid = str(uuid.uuid4())

        exchange = get_rpc_exchange(config)

        queue_name = RPC_REPLY_QUEUE_TEMPLATE.format(
            "standalone_rpc_proxy", self.uuid
        )
        queue = Queue(
            queue_name,
            exchange=exchange,
            routing_key=self.uuid,
            queue_arguments={
                'x-expires': RPC_REPLY_QUEUE_TTL
            }
        )

        self.reply_listener = ReplyListener(config, queue, timeout=timeout)

        self.serializer = serialization.setup(config).serializer

        serializer = publisher_options.pop('serializer', self.serializer)

        for option in RESTRICTED_PUBLISHER_OPTIONS:
            publisher_options.pop(option, None)

        ssl = self.config.get(AMQP_SSL_CONFIG_KEY)

        publisher = self.publisher_cls(
            self.amqp_uri,
            ssl=ssl,
            exchange=exchange,
            serializer=serializer,
            declare=[self.reply_listener.queue],
            reply_to=self.reply_listener.queue.routing_key,
            **publisher_options
        )

        data = context_data

        def publish(*args, **kwargs):

            context_data = data or {}
            context_data['call_id_stack'] = [
                'standalone_rpc_proxy.{}.{}'.format(self.uuid, new_call_id())
            ]

            extra_headers = encode_to_headers(context_data)

            publisher.publish(
                *args, extra_headers=extra_headers, **kwargs
            )

        get_reply = self.reply_listener.register_for_reply

        self.proxy = Proxy(publish, get_reply)

    @property
    def amqp_uri(self):
        return self.config[AMQP_URI_CONFIG_KEY]

    def __enter__(self):
        return self.start()

    def __exit__(self, tpe, value, traceback):
        self.stop()

    def start(self):
        self.reply_listener.start()
        return self.proxy

    def stop(self):
        self.reply_listener.stop()


class ServiceRpcProxy(RpcProxy):
    """
    Single-threaded RPC proxy to a named service.

    As per :class:`~nameko.standalone.rpc.RpcProxy` but with a pre-specified
    target service.
    """

    def __init__(self, service_name, *args, **kwargs):
        super(ServiceRpcProxy, self).__init__(*args, **kwargs)
        self.proxy = getattr(self.proxy, service_name)


ClusterRpcProxy = RpcProxy  # backwards compat
