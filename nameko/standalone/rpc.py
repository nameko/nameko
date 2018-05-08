from __future__ import absolute_import

import logging
import socket
import uuid

from amqp.exceptions import NotFound
from kombu.common import maybe_declare
from kombu.messaging import Queue

from nameko import serialization
from nameko.amqp import verify_amqp_uri
from nameko.amqp.consume import Consumer
from nameko.amqp.publish import Publisher
from nameko.constants import AMQP_URI_CONFIG_KEY
from nameko.containers import new_call_id
from nameko.exceptions import ReplyQueueExpiredWithPendingReplies, RpcTimeout
from nameko.messaging import encode_to_headers
from nameko.rpc import (
    RPC_REPLY_QUEUE_TEMPLATE, RPC_REPLY_QUEUE_TTL, Proxy, RpcReply,
    get_rpc_exchange
)


_logger = logging.getLogger(__name__)


class ReplyListener(Consumer):

    def __init__(self, config, queue, timeout=None, **kwargs):
        self.queue = queue
        self.timeout = timeout

        self.pending = {}

        super(ReplyListener, self).__init__(
            config=config,
            queues=[self.queue],
            callbacks=[self.handle_message],
            **kwargs
        )

    def start(self):
        verify_amqp_uri(self.amqp_uri)

        self.should_stop = False
        with self.connection as conn:
            maybe_declare(self.queue, conn)

    def stop(self):
        self.should_stop = True

    def get_consumers(self, consumer_cls, channel):
        """ Extend Consumer.get_consumers
        """
        if self.pending:
            try:
                with self.connection as conn:
                    self.queue.bind(conn).queue_declare(passive=True)
            except NotFound:
                raise ReplyQueueExpiredWithPendingReplies(
                    "Lost replies for correlation ids:\n{}".format(
                        "\n".join(self.pending.keys())
                    )
                )

        return super(ReplyListener, self).get_consumers(consumer_cls, channel)

    def register_for_reply(self, correlation_id=None):
        if correlation_id is None:
            correlation_id = str(uuid.uuid4())
        self.pending[correlation_id] = None
        return RpcReply(
            lambda: self.consume_reply(correlation_id), correlation_id
        )

    def consume_reply(self, correlation_id):
        # return error if correlation_id not pending? (new feature)
        if self.should_stop:
            raise RuntimeError("Stopped and can no longer be used")

        while not self.pending.get(correlation_id):
            try:
                next(self.consume(timeout=self.timeout))
            except socket.timeout:
                raise RpcTimeout()
        return self.pending.pop(correlation_id)

    def handle_message(self, body, message):
        self.ack_message(message)

        correlation_id = message.properties.get('correlation_id')
        if correlation_id not in self.pending:
            _logger.debug("Unknown correlation id: %s", correlation_id)
            return

        self.pending[correlation_id] = body


class RpcProxy(object):
    """
    A single-threaded RPC proxy to a cluster of services. Individual services
    are accessed via attributes, which return service proxies. Method calls on
    the proxies are converted into RPC calls to the service, with responses
    returned directly.

    Enables services not hosted by nameko to make RPC requests to a nameko
    cluster. It is commonly used as a context manager but may also be manually
    started and stopped.

    This is similar to the service proxy, but may be uses a single reply queue
    for calls to all services, where a collection of service proxies would have
    one reply queue per proxy.

    *Usage*

    As a context manager::

        with RpcProxy(config) as proxy:
            proxy.service.method()
            proxy.other_service.method()

    The equivalent call, manually starting and stopping::

        proxy = RpcProxy(config)
        proxy = proxy.start()
        try:
            proxy.targetservice.method()
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

    def __init__(self, config, context_data=None, timeout=None, **options):
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

        self.serializer, _ = serialization.setup(config)

        serializer = options.pop('serializer', self.serializer)

        publisher = Publisher(
            self.amqp_uri,
            serializer=serializer,
            **options
        )

        data = context_data

        def publish(*args, **kwargs):

            context_data = data or {}
            context_data['call_id_stack'] = [
                'standalone_rpc_proxy.{}.{}'.format(self.uuid, new_call_id())
            ]

            extra_headers = encode_to_headers(context_data)

            publisher.publish(
                *args,
                exchange=exchange,
                declare=[self.reply_listener.queue],
                reply_to=self.reply_listener.queue.routing_key,
                extra_headers=extra_headers,
                **kwargs
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
    A single-threaded RPC proxy to a named service. As per
    `~nameko.standalone.rpc.RpcProxy` but with a pre-specified target service.
    """

    def __init__(self, service_name, *args, **kwargs):
        super(ServiceRpcProxy, self).__init__(*args, **kwargs)
        self.proxy = getattr(self.proxy, service_name)


ClusterRpcProxy = RpcProxy  # backwards compat
