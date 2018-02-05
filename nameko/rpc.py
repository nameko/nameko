from __future__ import absolute_import, unicode_literals

import sys
import uuid
from functools import partial
from logging import getLogger

import kombu.serialization
from eventlet.event import Event
from kombu import Exchange, Queue

from nameko.amqp.publish import Publisher, UndeliverableMessage
from nameko.constants import (
    AMQP_URI_CONFIG_KEY, DEFAULT_SERIALIZER, RPC_EXCHANGE_CONFIG_KEY,
    SERIALIZER_CONFIG_KEY
)
from nameko.exceptions import (
    ContainerBeingKilled, MalformedRequest, MethodNotFound, UnknownService,
    UnserializableValueError, deserialize, serialize
)
from nameko.extensions import (
    DependencyProvider, Entrypoint, ProviderCollector, SharedExtension
)
from nameko.messaging import HeaderDecoder, HeaderEncoder, QueueConsumer


_log = getLogger(__name__)


RPC_QUEUE_TEMPLATE = 'rpc-{}'
RPC_REPLY_QUEUE_TEMPLATE = 'rpc.reply-{}-{}'
RPC_REPLY_QUEUE_TTL = 300000  # ms (5 mins)


def get_rpc_exchange(config):
    # TODO: refactor this ugliness
    exchange_name = config.get(RPC_EXCHANGE_CONFIG_KEY, 'nameko-rpc')
    exchange = Exchange(exchange_name, durable=True, type="topic")
    return exchange


class RpcConsumer(SharedExtension, ProviderCollector):

    queue_consumer = QueueConsumer()

    def __init__(self):
        self._unregistering_providers = set()
        self._unregistered_from_queue_consumer = Event()
        self.queue = None
        super(RpcConsumer, self).__init__()

    def setup(self):
        if self.queue is None:

            service_name = self.container.service_name
            queue_name = RPC_QUEUE_TEMPLATE.format(service_name)
            routing_key = '{}.*'.format(service_name)

            exchange = get_rpc_exchange(self.container.config)

            self.queue = Queue(
                queue_name,
                exchange=exchange,
                routing_key=routing_key,
                durable=True)

            self.queue_consumer.register_provider(self)
            self._registered = True

    def stop(self):
        """ Stop the RpcConsumer.

        The RpcConsumer ordinary unregisters from the QueueConsumer when the
        last Rpc subclass unregisters from it. If no providers were registered,
        we should unregister from the QueueConsumer as soon as we're asked
        to stop.
        """
        if not self._providers_registered:
            self.queue_consumer.unregister_provider(self)
            self._unregistered_from_queue_consumer.send(True)

    def unregister_provider(self, provider):
        """ Unregister a provider.

        Blocks until this RpcConsumer is unregistered from its QueueConsumer,
        which only happens when all providers have asked to unregister.
        """
        self._unregistering_providers.add(provider)
        remaining_providers = self._providers - self._unregistering_providers
        if not remaining_providers:
            _log.debug('unregistering from queueconsumer %s', self)
            self.queue_consumer.unregister_provider(self)
            _log.debug('unregistered from queueconsumer %s', self)
            self._unregistered_from_queue_consumer.send(True)

        _log.debug('waiting for unregister from queue consumer %s', self)
        self._unregistered_from_queue_consumer.wait()
        super(RpcConsumer, self).unregister_provider(provider)

    def get_provider_for_method(self, routing_key):
        service_name = self.container.service_name

        for provider in self._providers:
            key = '{}.{}'.format(service_name, provider.method_name)
            if key == routing_key:
                return provider
        else:
            method_name = routing_key.split(".")[-1]
            raise MethodNotFound(method_name)

    def handle_message(self, body, message):
        routing_key = message.delivery_info['routing_key']
        try:
            provider = self.get_provider_for_method(routing_key)
            provider.handle_message(body, message)
        except Exception:
            exc_info = sys.exc_info()
            self.handle_result(message, None, exc_info)

    def handle_result(self, message, result, exc_info):

        amqp_uri = self.container.config[AMQP_URI_CONFIG_KEY]
        serializer = self.container.config.get(
            SERIALIZER_CONFIG_KEY, DEFAULT_SERIALIZER
        )
        exchange = get_rpc_exchange(self.container.config)

        responder = Responder(amqp_uri, exchange, serializer, message)
        result, exc_info = responder.send_response(result, exc_info)

        self.queue_consumer.ack_message(message)
        return result, exc_info

    def requeue_message(self, message):
        self.queue_consumer.requeue_message(message)


class Rpc(Entrypoint, HeaderDecoder):

    rpc_consumer = RpcConsumer()

    def setup(self):
        self.rpc_consumer.register_provider(self)

    def stop(self):
        self.rpc_consumer.unregister_provider(self)

    def handle_message(self, body, message):
        try:
            args = body['args']
            kwargs = body['kwargs']
        except KeyError:
            raise MalformedRequest('Message missing `args` or `kwargs`')

        self.check_signature(args, kwargs)

        context_data = self.unpack_message_headers(message)

        handle_result = partial(self.handle_result, message)
        try:
            self.container.spawn_worker(self, args, kwargs,
                                        context_data=context_data,
                                        handle_result=handle_result)
        except ContainerBeingKilled:
            self.rpc_consumer.requeue_message(message)

    def handle_result(self, message, worker_ctx, result, exc_info):
        result, exc_info = self.rpc_consumer.handle_result(
            message, result, exc_info)
        return result, exc_info


rpc = Rpc.decorator


class Responder(object):

    publisher_cls = Publisher

    def __init__(self, amqp_uri, exchange, serializer, message):
        self.amqp_uri = amqp_uri
        self.serializer = serializer
        self.message = message
        self.exchange = exchange

    def send_response(self, result, exc_info):

        error = None
        if exc_info is not None:
            error = serialize(exc_info[1])

        # disaster avoidance serialization check: `result` must be
        # serializable, otherwise the container will commit suicide assuming
        # unrecoverable errors (and the message will be requeued for another
        # victim)

        try:
            kombu.serialization.dumps(result, self.serializer)
        except Exception:
            exc_info = sys.exc_info()
            # `error` below is guaranteed to serialize to json
            error = serialize(UnserializableValueError(result))
            result = None

        payload = {'result': result, 'error': error}

        routing_key = self.message.properties['reply_to']
        correlation_id = self.message.properties.get('correlation_id')

        publisher = self.publisher_cls(self.amqp_uri)

        publisher.publish(
            payload,
            serializer=self.serializer,
            exchange=self.exchange,
            routing_key=routing_key,
            correlation_id=correlation_id
        )

        return result, exc_info


class ReplyListener(SharedExtension):

    queue_consumer = QueueConsumer()

    def __init__(self, **kwargs):
        self.pending = {}
        super(ReplyListener, self).__init__(**kwargs)

    def setup(self):

        reply_queue_uuid = uuid.uuid4()
        service_name = self.container.service_name

        queue_name = RPC_REPLY_QUEUE_TEMPLATE.format(
            service_name, reply_queue_uuid)

        self.routing_key = str(reply_queue_uuid)

        exchange = get_rpc_exchange(self.container.config)

        self.queue = Queue(
            queue_name,
            exchange=exchange,
            routing_key=self.routing_key,
            queue_arguments={
                'x-expires': RPC_REPLY_QUEUE_TTL
            }
        )

        self.queue_consumer.register_provider(self)

    def stop(self):
        self.queue_consumer.unregister_provider(self)
        super(ReplyListener, self).stop()

    def register_for_reply(self, correlation_id=None):
        if correlation_id is None:
            correlation_id = str(uuid.uuid4())

        reply_event = Event()
        self.pending[correlation_id] = reply_event

        return RpcReply(reply_event.wait, correlation_id)

    def handle_message(self, body, message):
        self.queue_consumer.ack_message(message)

        correlation_id = message.properties.get('correlation_id')
        reply_event = self.pending.pop(correlation_id, None)
        if reply_event is not None:
            reply_event.send(body)
        else:
            _log.debug("Unknown correlation id: %s", correlation_id)


class RpcProxy(DependencyProvider, HeaderEncoder):

    publisher_cls = Publisher

    reply_listener = ReplyListener()

    def __init__(self, target_service, **options):
        self.target_service = target_service
        self.options = options

    @property
    def amqp_uri(self):
        return self.container.config[AMQP_URI_CONFIG_KEY]

    def setup(self):
        self.exchange = get_rpc_exchange(self.container.config)
        self.publisher = self.publisher_cls(
            self.amqp_uri,
            **self.options
        )

    def get_dependency(self, worker_ctx):

        extra_headers = self.get_message_headers(worker_ctx)

        publish = partial(
            self.publisher.publish,
            exchange=self.exchange,
            reply_to=self.reply_listener.routing_key,
            extra_headers=extra_headers
        )

        get_reply = self.reply_listener.register_for_reply

        proxy = Proxy(publish, get_reply)
        return getattr(proxy, self.target_service)


class Proxy(object):

    def __init__(
        self, publish, get_reply, service_name=None, method_name=None
    ):
        self.publish = publish
        self.get_reply = get_reply
        self.service_name = service_name
        self.method_name = method_name

    def __getattr__(self, name):
        if self.method_name is not None:
            raise AttributeError(name)

        clone = Proxy(
            self.publish,
            self.get_reply,
            self.service_name or name,
            self.service_name and name
        )
        return clone

    @property
    def fully_specified(self):
        return (
            self.service_name is not None and self.method_name is not None
        )

    @property
    def identifier(self):
        return "{}.{}".format(
            self.service_name or "*",
            self.method_name or "*"
        )

    def __getitem__(self, name):
        """Enable dict-like access on the proxy. """
        return getattr(self, name)

    def __call__(self, *args, **kwargs):
        reply = self._call(*args, **kwargs)
        return reply.result()

    def call_async(self, *args, **kwargs):
        reply = self._call(*args, **kwargs)
        return reply

    def _call(self, *args, **kwargs):
        if not self.fully_specified:
            raise ValueError(
                "Cannot call unspecified method {}".format(self.identifier)
            )

        _log.debug('invoking %s', self)

        msg = {'args': args, 'kwargs': kwargs}

        # We use the `mandatory` flag in `producer.publish` below to catch rpc
        # calls to non-existent services, which would otherwise wait forever
        # for a reply that will never arrive.
        #
        # However, the basic.return ("no one is listening for topic") is sent
        # asynchronously and conditionally, so we can't wait() on the channel
        # for it (will wait forever on successful delivery).
        #
        # Instead, we make use of (the rabbitmq extension) confirm_publish
        # (https://www.rabbitmq.com/confirms.html), which _always_ sends a
        # reply down the channel. Moreover, in the case where no queues are
        # bound to the exchange (service unknown), the basic.return is sent
        # first, so by the time kombu returns (after waiting for the confim)
        # we can reliably check for returned messages.

        # Note that deactivating publish-confirms in the RpcProxy will disable
        # this functionality and therefore :class:`UnknownService` will never
        # be raised (and the caller will hang).

        routing_key = self.identifier

        reply = self.get_reply()

        try:
            self.publish(
                msg,
                routing_key=routing_key,
                mandatory=True,
                correlation_id=reply.correlation_id
            )
        except UndeliverableMessage:
            raise UnknownService(self.service_name)

        return reply

    def __repr__(self):
        return '<RpcProxy: {}>'.format(self.identifier)


class RpcReply(object):
    def __init__(self, wait, correlation_id):
        self.response = None
        self.wait = wait
        self.correlation_id = correlation_id

    def result(self):
        if self.response is None:
            self.response = self.wait()
        error = self.response.get('error')
        if error:
            raise deserialize(error)
        return self.response['result']
