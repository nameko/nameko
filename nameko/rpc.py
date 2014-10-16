from __future__ import absolute_import
from functools import partial
import inspect
import json
from logging import getLogger
import sys
import uuid

from eventlet.event import Event
from kombu import Connection, Exchange, Queue
from kombu.pools import producers

from nameko.constants import DEFAULT_RETRY_POLICY
from nameko.exceptions import (
    MethodNotFound, UnknownService, UnserializableValueError,
    serialize, deserialize)
from nameko.messaging import (
    queue_consumer, HeaderEncoder, HeaderDecoder, AMQP_URI_CONFIG_KEY)
from nameko.dependencies import (
    entrypoint, injection, InjectionProvider, EntrypointProvider,
    DependencyFactory, dependency, ProviderCollector, DependencyProvider,
    CONTAINER_SHARED, ConfigValue)
from nameko.exceptions import IncorrectSignature, ContainerBeingKilled

_log = getLogger(__name__)


RPC_EXCHANGE_CONFIG_KEY = 'rpc_exchange'
RPC_QUEUE_TEMPLATE = 'rpc-{}'
RPC_REPLY_QUEUE_TEMPLATE = 'rpc.reply-{}-{}'


def get_rpc_exchange(container):
    exchange_name = container.config.get(RPC_EXCHANGE_CONFIG_KEY, 'nameko-rpc')
    exchange = Exchange(exchange_name, durable=True, type="topic")
    return exchange


# pylint: disable=E1101,E1123
class RpcConsumer(DependencyProvider, ProviderCollector):

    queue_consumer = queue_consumer(shared=CONTAINER_SHARED)

    def __init__(self):
        super(RpcConsumer, self).__init__()
        self._unregistering_providers = set()
        self._unregistered_from_queue_consumer = Event()
        self.queue = None

    def prepare(self):
        if self.queue is None:

            container = self.container
            service_name = container.service_name
            queue_name = RPC_QUEUE_TEMPLATE.format(service_name)
            routing_key = '{}.*'.format(service_name)
            exchange = get_rpc_exchange(container)

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
        last RpcProvider unregisters from it. If no providers were registered,
        we should unregister ourself from the QueueConsumer as soon as we're
        asked to stop.
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
            key = '{}.{}'.format(service_name, provider.name)
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
        except (MethodNotFound, IncorrectSignature):
            exc_info = sys.exc_info()
            self.handle_result(message, self.container, None, exc_info)

    def handle_result(self, message, container, result, exc_info):
        responder = Responder(message)
        result, exc_info = responder.send_response(container, result, exc_info)

        self.queue_consumer.ack_message(message)
        return result, exc_info

    def requeue_message(self, message):
        self.queue_consumer.requeue_message(message)


@dependency
def rpc_consumer():
    return DependencyFactory(RpcConsumer)


# pylint: disable=E1101,E1123
class RpcProvider(EntrypointProvider, HeaderDecoder):

    rpc_consumer = rpc_consumer(shared=CONTAINER_SHARED)

    # config
    uri = ConfigValue(None)
    exchange = ConfigValue(None)

    def __init__(self, uri=None, exchange=None):
        if uri is None:
            uri = lambda container: container.config[AMQP_URI_CONFIG_KEY]
        self.uri = uri

        if exchange is None:
            exchange = lambda container: (
                container.config.get[RPC_EXCHANGE_CONFIG_KEY])
        self.exchange = exchange

    def prepare(self):
        self.rpc_consumer.register_provider(self)

    def stop(self):
        self.rpc_consumer.unregister_provider(self)
        super(RpcProvider, self).stop()

    def check_signature(self, args, kwargs):
        service_cls = self.container.service_cls
        fn = getattr(service_cls, self.name)
        try:
            service_instance = None  # fn is unbound
            inspect.getcallargs(fn, service_instance, *args, **kwargs)
        except TypeError as exc:
            raise IncorrectSignature(str(exc))

    def handle_message(self, body, message):
        args = body['args']
        kwargs = body['kwargs']

        self.check_signature(args, kwargs)

        worker_ctx_cls = self.container.worker_ctx_cls
        context_data = self.unpack_message_headers(worker_ctx_cls, message)

        handle_result = partial(self.handle_result, message)
        try:
            self.container.spawn_worker(self, args, kwargs,
                                        context_data=context_data,
                                        handle_result=handle_result)
        except ContainerBeingKilled:
            self.rpc_consumer.requeue_message(message)

    def handle_result(self, message, worker_ctx, result, exc_info):
        container = self.container
        result, exc_info = self.rpc_consumer.handle_result(
            message, container, result, exc_info)
        return result, exc_info


@entrypoint
def rpc(uri=None, exchange=None):
    return DependencyFactory(RpcProvider, uri, exchange)


class Responder(object):

    def __init__(self, message):
        self.message = message

    def send_response(self, container, result, exc_info, **kwargs):

        error = None
        if exc_info is not None:
            error = serialize(exc_info[1])

        # disaster avoidance serialization check
        # `result` and `error` must both be json serializable, otherwise
        # the container will commit suicide assuming unrecoverable errors
        # (and the message will be requeued for another victim)
        for item in (result, error):
            try:
                json.dumps(item)
            except Exception:
                result = None
                exc_info = sys.exc_info()
                # `error` below is guaranteed to serialize to json
                error = serialize(UnserializableValueError(item))

        conn = Connection(container.config[AMQP_URI_CONFIG_KEY])

        exchange = get_rpc_exchange(container)

        retry = kwargs.pop('retry', True)
        retry_policy = kwargs.pop('retry_policy', DEFAULT_RETRY_POLICY)

        with producers[conn].acquire(block=True) as producer:

            routing_key = self.message.properties['reply_to']
            correlation_id = self.message.properties.get('correlation_id')

            msg = {'result': result, 'error': error}

            _log.debug('publish response %s:%s', routing_key, correlation_id)
            producer.publish(
                msg, retry=retry, retry_policy=retry_policy,
                exchange=exchange, routing_key=routing_key,
                correlation_id=correlation_id, **kwargs)

        return result, exc_info


# pylint: disable=E1101,E1123
class ReplyListener(DependencyProvider):

    queue_consumer = queue_consumer(shared=CONTAINER_SHARED)

    def __init__(self):
        super(ReplyListener, self).__init__()
        self._reply_events = {}

    def prepare(self):

        service_uuid = uuid.uuid4()  # TODO: give srv_ctx a uuid?
        container = self.container

        service_name = container.service_name

        queue_name = RPC_REPLY_QUEUE_TEMPLATE.format(
            service_name, service_uuid)

        self.routing_key = str(service_uuid)

        exchange = get_rpc_exchange(container)

        self.queue = Queue(queue_name, exchange=exchange,
                           routing_key=self.routing_key)

        self.queue_consumer.register_provider(self)

    def stop(self):
        self.queue_consumer.unregister_provider(self)
        super(ReplyListener, self).stop()

    def get_reply_event(self, correlation_id):
        reply_event = Event()
        self._reply_events[correlation_id] = reply_event
        return reply_event

    def handle_message(self, body, message):
        self.queue_consumer.ack_message(message)

        correlation_id = message.properties.get('correlation_id')
        client_event = self._reply_events.pop(correlation_id, None)
        if client_event is not None:
            client_event.send(body)
        else:
            _log.debug("Unknown correlation id: %s", correlation_id)


@dependency
def reply_listener():
    return DependencyFactory(ReplyListener)


class RpcProxyProvider(InjectionProvider):

    rpc_reply_listener = reply_listener(shared=CONTAINER_SHARED)

    # config
    uri = ConfigValue(None)
    exchange = ConfigValue(None)

    def __init__(self, service_name, uri=None, exchange=None):
        self.service_name = service_name

        if uri is None:
            uri = lambda container: container.config[AMQP_URI_CONFIG_KEY]
        self.uri = uri

        if exchange is None:
            exchange = lambda container: (
                container.config.get[RPC_EXCHANGE_CONFIG_KEY])
        self.exchange = exchange

    def acquire_injection(self, worker_ctx):
        return ServiceProxy(worker_ctx, self.service_name,
                            self.rpc_reply_listener)


@injection
def rpc_proxy(service_name, uri=None, exchange=None):
    return DependencyFactory(RpcProxyProvider, service_name, uri, exchange)


class ServiceProxy(object):
    def __init__(self, worker_ctx, service_name, reply_listener):
        self.worker_ctx = worker_ctx
        self.service_name = service_name
        self.reply_listener = reply_listener

    def __getattr__(self, name):
        return MethodProxy(
            self.worker_ctx, self.service_name, name, self.reply_listener)


class MethodProxy(HeaderEncoder):

    def __init__(self, worker_ctx, service_name, method_name, reply_listener):
        self.worker_ctx = worker_ctx
        self.service_name = service_name
        self.method_name = method_name
        self.reply_listener = reply_listener

    def __call__(self, *args, **kwargs):
        _log.debug('invoking %s', self)

        worker_ctx = self.worker_ctx
        container = worker_ctx.container

        msg = {'args': args, 'kwargs': kwargs}

        conn = Connection(
            container.config[AMQP_URI_CONFIG_KEY],
            transport_options={'confirm_publish': True},
        )

        # We use the `mandatory` flag in `producer.publish` below to catch rpc
        # calls to non-existent services, which would otherwise wait forever
        # for a reply that will never arrive.
        #
        # However, the basic.return ("no one is listening for topic") is sent
        # asynchronously and conditionally, so we can't wait() on the channel
        # for it (will wait forever on successful delivery).
        #
        # Instead, we use (the rabbitmq extension) confirm_publish
        # (https://www.rabbitmq.com/confirms.html), which _always_ sends a
        # reply down the channel. Moreover, in the case where no queues are
        # bound to the exchange (service unknown), the basic.return is sent
        # first, so by the time kombu returns (after waiting for the confim)
        # we can reliably check for returned messages.

        routing_key = '{}.{}'.format(self.service_name, self.method_name)

        exchange = get_rpc_exchange(container)

        with producers[conn].acquire(block=True) as producer:

            headers = self.get_message_headers(worker_ctx)
            correlation_id = str(uuid.uuid4())

            reply_listener = self.reply_listener
            reply_to_routing_key = reply_listener.routing_key
            reply_event = reply_listener.get_reply_event(correlation_id)

            producer.publish(
                msg,
                exchange=exchange,
                routing_key=routing_key,
                mandatory=True,
                reply_to=reply_to_routing_key,
                headers=headers,
                correlation_id=correlation_id,
                retry=True,
                retry_policy=DEFAULT_RETRY_POLICY
            )

            if not producer.channel.returned_messages.empty():
                raise UnknownService(self.service_name)

        _log.debug('Waiting for RPC reply event %s', self)
        resp_body = reply_event.wait()
        _log.debug('RPC reply event complete %s %s', self, resp_body)

        error = resp_body.get('error')
        if error:
            raise deserialize(error)
        return resp_body['result']

    def __str__(self):
        return '<proxy method: %s.%s>' % (self.service_name, self.method_name)
