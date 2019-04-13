from __future__ import absolute_import

import logging
import socket
import uuid

from amqp.exceptions import NotFound
from kombu.common import maybe_declare
from kombu.messaging import Queue

from nameko import config, serialization
from nameko.amqp.consume import Consumer
from nameko.amqp.publish import Publisher, get_connection
from nameko.constants import (
    AMQP_SSL_CONFIG_KEY, AMQP_URI_CONFIG_KEY, CALL_ID_STACK_CONTEXT_KEY,
    DEFAULT_AMQP_URI, DEFAULT_HEARTBEAT, DEFAULT_PREFETCH_COUNT,
    HEARTBEAT_CONFIG_KEY, PREFETCH_COUNT_CONFIG_KEY
)
from nameko.containers import new_call_id
from nameko.exceptions import ReplyQueueExpiredWithPendingReplies, RpcTimeout
from nameko.messaging import encode_to_headers
from nameko.rpc import (
    RESTRICTED_PUBLISHER_OPTIONS, RPC_REPLY_QUEUE_TEMPLATE,
    RPC_REPLY_QUEUE_TTL, Client, get_rpc_exchange
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

    def __init__(self, queue, timeout=None, uri=None, ssl=None, **kwargs):
        self.queue = queue
        self.timeout = timeout

        self.amqp_uri = uri or config.get(AMQP_URI_CONFIG_KEY, DEFAULT_AMQP_URI)
        self.ssl = ssl if ssl is not None else config.get(AMQP_SSL_CONFIG_KEY)

        self.pending = {}

    def start(self):
        heartbeat = config.get(HEARTBEAT_CONFIG_KEY, DEFAULT_HEARTBEAT)
        prefetch_count = config.get(
            PREFETCH_COUNT_CONFIG_KEY, DEFAULT_PREFETCH_COUNT
        )
        accept = serialization.setup().accept

        queues = [self.queue]
        callbacks = [self.handle_message]

        self.consumer = self.consumer_cls(
            self.check_for_lost_replies, self.amqp_uri, ssl=self.ssl,
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
                with get_connection(self.amqp_uri, ssl=self.ssl) as conn:
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


class ClusterRpcClient(object):
    """
    Single-threaded RPC client to a cluster of services. The target service
    and method are specified with attributes.

    Method calls on the local object are converted into RPC calls to the
    target service.

    Enables services not hosted by nameko to make RPC requests to a nameko
    cluster. It is commonly used as a context manager but may also be manually
    started and stopped.

    *Usage*

    As a context manager::

        with ClusterRpc() as client:
            client.target_service.method()
            client.other_service.method()

    The equivalent call, manually starting and stopping::

        client = ClusterRpc()
        client = client.start()
        try:
            client.target_service.method()
            client.other_service.method()
        finally:
            client.stop()

    If you call ``start()`` you must eventually call ``stop()`` to close the
    connection to the broker.

    You may also supply ``context_data``, a dictionary of data to be
    serialised into the AMQP message headers.

    When the name of the service is not legal in Python, you can also
    use a dict-like syntax::

        with ClusterRpc() as client:
            client['service-name'].method()
            client['other-service'].method()

    """

    publisher_cls = Publisher

    def __init__(
        self, context_data=None, timeout=None, **publisher_options
    ):
        self.uuid = str(uuid.uuid4())

        exchange = get_rpc_exchange()

        queue_name = RPC_REPLY_QUEUE_TEMPLATE.format(
            "standalone_rpc_client", self.uuid
        )
        queue = Queue(
            queue_name,
            exchange=exchange,
            routing_key=self.uuid,
            queue_arguments={
                'x-expires': RPC_REPLY_QUEUE_TTL
            }
        )

        self.amqp_uri = publisher_options.pop(
            'uri', config.get(AMQP_URI_CONFIG_KEY, DEFAULT_AMQP_URI)
        )
        self.ssl = publisher_options.pop(
            'ssl', config.get(AMQP_SSL_CONFIG_KEY)
        )

        self.reply_listener = ReplyListener(queue, timeout=timeout)

        serialization_config = serialization.setup()
        self.serializer = publisher_options.pop(
            'serializer', serialization_config.serializer
        )

        for option in RESTRICTED_PUBLISHER_OPTIONS:
            publisher_options.pop(option, None)

        publisher = self.publisher_cls(
            self.amqp_uri,
            ssl=self.ssl,
            exchange=exchange,
            serializer=self.serializer,
            declare=[self.reply_listener.queue],
            reply_to=self.reply_listener.queue.routing_key,
            **publisher_options
        )

        context_data = context_data or {}

        def publish(*args, **kwargs):

            context_data[CALL_ID_STACK_CONTEXT_KEY] = [
                'standalone_rpc_client.{}.{}'.format(self.uuid, new_call_id())
            ]

            extra_headers = kwargs.pop('extra_headers')
            extra_headers.update(encode_to_headers(context_data))

            publisher.publish(
                *args, extra_headers=extra_headers, **kwargs
            )

        get_reply = self.reply_listener.register_for_reply

        self.client = Client(publish, get_reply, context_data)

    def __enter__(self):
        return self.start()

    def __exit__(self, tpe, value, traceback):
        self.stop()

    def start(self):
        self.reply_listener.start()
        return self.client

    def stop(self):
        self.reply_listener.stop()


class ServiceRpcClient(ClusterRpcClient):
    """
    Single-threaded RPC client to a named service.

    As per :class:`~nameko.standalone.rpc.ClusterRpc` but with a pre-specified
    target service.
    """

    def __init__(self, service_name, *args, **kwargs):
        super(ServiceRpcClient, self).__init__(*args, **kwargs)
        self.client = getattr(self.client, service_name)


ClusterRpcProxy = ClusterRpcClient  # backwards compat
ServiceRpcProxy = ServiceRpcClient  # backwards compat
