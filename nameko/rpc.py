from __future__ import absolute_import
from functools import partial
from logging import getLogger
import uuid

from eventlet.event import Event
from kombu import Connection, Exchange, Queue
from kombu.pools import producers

from nameko.exceptions import MethodNotFound, RemoteErrorWrapper
from nameko.messaging import (
    queue_consumer, HeaderEncoder, HeaderDecoder, AMQP_URI_CONFIG_KEY)
from nameko.dependencies import (
    entrypoint, injection, InjectionProvider, EntrypointProvider,
    DependencyFactory, dependency, ProviderCollector, DependencyProvider,
    CONTAINER_SHARED)

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

    def stop(self):
        _log.debug('waiting for providers to unregister %s', self)
        self._last_provider_unregistered.wait()
        _log.debug('all providers unregistered %s', self)
        self.queue_consumer.unregister_provider(self)

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
        except MethodNotFound as exc:
            self.handle_result(message, self.container, None, exc)

    def handle_result(self, message, container, result, exc):
        error = None
        if exc is not None:
            # TODO: this is helpful for debug, but shouldn't be used in
            # production (since it exposes the callee's internals).
            # Replace this when we can correlate exceptions properly.
            error = RemoteErrorWrapper(exc)

        responder = Responder(message)
        responder.send_response(container, result, error)

        self.queue_consumer.ack_message(message)


@dependency
def rpc_consumer():
    return DependencyFactory(RpcConsumer)


# pylint: disable=E1101,E1123
class RpcProvider(EntrypointProvider, HeaderDecoder):

    rpc_consumer = rpc_consumer(shared=CONTAINER_SHARED)

    def prepare(self):
        self.rpc_consumer.register_provider(self)

    def stop(self):
        self.rpc_consumer.unregister_provider(self)
        super(RpcProvider, self).stop()

    def handle_message(self, body, message):
        args = body['args']
        kwargs = body['kwargs']

        worker_ctx_cls = self.container.worker_ctx_cls
        context_data = self.unpack_message_headers(worker_ctx_cls, message)

        handle_result = partial(self.handle_result, message)
        self.container.spawn_worker(self, args, kwargs,
                                    context_data=context_data,
                                    handle_result=handle_result)

    def handle_result(self, message, worker_ctx, result, exc):
        self.rpc_consumer.handle_result(message, self.container, result, exc)


@entrypoint
def rpc():
    return DependencyFactory(RpcProvider)


class Responder(object):
    def __init__(self, message, retry=True, retry_policy=None):
        self._connection = None
        self.message = message
        self.retry = retry
        if retry_policy is None:
            retry_policy = {'max_retries': 3}
        self.retry_policy = retry_policy

    def connection_factory(self, container):
        return Connection(container.config[AMQP_URI_CONFIG_KEY])

    def send_response(self, container, result, error_wrapper):

        # TODO: if we use error codes outside the payload we would only
        # need to serialize the actual value
        # assumes result is json-serializable
        error = None
        if error_wrapper is not None:
            error = error_wrapper.serialize()

        with self.connection_factory(container) as conn:

            with producers[conn].acquire(block=True) as producer:

                reply_to = self.message.properties['reply_to']
                correlation_id = self.message.properties.get('correlation_id')

                msg = {'result': result, 'error': error}

                # all queues are bound to the anonymous direct exchange
                producer.publish(
                    msg, retry=self.retry, retry_policy=self.retry_policy,
                    routing_key=reply_to, correlation_id=correlation_id)


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
        self.reply_queue_name = RPC_REPLY_QUEUE_TEMPLATE.format(
            service_name, service_uuid)

        exchange = get_rpc_exchange(container)
        self.queue = Queue(
            self.reply_queue_name, exchange=exchange, exclusive=True)

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

    def __init__(self, service_name):
        self.service_name = service_name

    def acquire_injection(self, worker_ctx):
        return ServiceProxy(worker_ctx, self.service_name,
                            self.rpc_reply_listener)


@injection
def rpc_proxy(service_name):
    return DependencyFactory(RpcProxyProvider, service_name)


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

        conn = Connection(container.config[AMQP_URI_CONFIG_KEY])
        routing_key = '{}.{}'.format(self.service_name, self.method_name)

        # TODO: should connection sharing be done during worker_setup in the
        #       dependency provider?
        with conn as conn:
            exchange = get_rpc_exchange(container)

            with producers[conn].acquire(block=True) as producer:
                # TODO: should we enable auto-retry,
                #      should that be an option in __init__?

                headers = self.get_message_headers(worker_ctx)
                correlation_id = str(uuid.uuid4())

                reply_listener = self.reply_listener
                reply_queue_name = reply_listener.reply_queue_name
                reply_event = reply_listener.get_reply_event(correlation_id)

                producer.publish(
                    msg,
                    exchange=exchange,
                    routing_key=routing_key,
                    reply_to=reply_queue_name,
                    headers=headers,
                    correlation_id=correlation_id,
                )

            resp_body = reply_event.wait()

            error = resp_body.get('error')
            if error:
                raise RemoteErrorWrapper.deserialize(error)
            return resp_body['result']

    def __str__(self):
        return '<proxy method: %s.%s>' % (self.service_name, self.method_name)
