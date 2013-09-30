from __future__ import absolute_import
from functools import partial
from logging import getLogger
import uuid
from weakref import WeakKeyDictionary

from kombu import Connection, Exchange, Queue
from kombu.pools import producers
from kombu.common import itermessages, maybe_declare

from nameko.exceptions import MethodNotFound, RemoteErrorWrapper
from nameko.messaging import (
    get_queue_consumer, HeaderEncoder, HeaderDecoder, AMQP_URI_CONFIG_KEY)
from nameko.dependencies import (
    dependency_decorator, AttributeDependency, DecoratorDependency)


_log = getLogger(__name__)


@dependency_decorator
def rpc():
    return RpcProvider()

RPC_EXCHANGE_CONFIG_KEY = 'rpc_exchange'
RPC_QUEUE_TEMPLATE = 'rpc-{}'
RPC_REPLY_QUEUE_TEMPLATE = 'rpc.reply-{}-{}'

rpc_consumers = WeakKeyDictionary()


def get_rpc_consumer(srv_ctx, consumer_cls):
    """ Get or create an RpcConsumer instance for the given ``srv_ctx``
    """
    if srv_ctx not in rpc_consumers:
        rpc_consumer = consumer_cls(srv_ctx)
        rpc_consumers[srv_ctx] = rpc_consumer

    return rpc_consumers[srv_ctx]


def get_rpc_exchange(srv_ctx):
    exchange_name = srv_ctx.config.get(RPC_EXCHANGE_CONFIG_KEY, 'nameko-rpc')
    exchange = Exchange(exchange_name, durable=True, type="topic")
    return exchange


class RpcConsumer(object):

    def __init__(self, srv_ctx):
        self._queue = None
        self._providers = {}
        self._srv_ctx = srv_ctx

    def prepare_queue(self):
        if self._queue is None:

            srv_ctx = self._srv_ctx
            service_name = srv_ctx.name
            queue_name = RPC_QUEUE_TEMPLATE.format(service_name)
            routing_key = '{}.*'.format(service_name)
            exchange = get_rpc_exchange(srv_ctx)

            self._queue = Queue(
                queue_name,
                exchange=exchange,
                routing_key=routing_key,
                durable=True)

            qc = get_queue_consumer(srv_ctx)
            qc.add_consumer(self._queue, self.handle_message)

    def start(self):
        qc = get_queue_consumer(self._srv_ctx)
        qc.start()

    def stop(self):
        qc = get_queue_consumer(self._srv_ctx)
        qc.stop()

    def register_provider(self, rpc_provider):
        service_name = self._srv_ctx.name
        key = '{}.{}'.format(service_name, rpc_provider.name)
        self._providers[key] = rpc_provider

    def unregister_provider(self, rpc_provider):
        service_name = self._srv_ctx.name
        key = '{}.{}'.format(service_name, rpc_provider.name)
        try:
            del self._providers[key]
        except KeyError:
            pass  # not registered

    def get_provider_for_method(self, routing_key):
        try:
            return self._providers[routing_key]
        except KeyError:
            method_name = routing_key.split(".")[-1]
            raise MethodNotFound(method_name)

    def handle_message(self, body, message):
        routing_key = message.delivery_info['routing_key']
        srv_ctx = self._srv_ctx
        try:
            provider = self.get_provider_for_method(routing_key)
            provider.handle_message(srv_ctx, body, message)
        except MethodNotFound as exc:
            self.handle_result(message, srv_ctx, None, exc)

    def handle_result(self, message, srv_ctx, result, exc):
        error = None
        if exc is not None:
            error = RemoteErrorWrapper(exc)

        responder = Responder(message)
        responder.send_response(srv_ctx, result, error)

        qc = get_queue_consumer(srv_ctx)
        qc.ack_message(message)


class RpcProvider(DecoratorDependency, HeaderDecoder):
    _consumer_cls = RpcConsumer

    def get_consumer(self, srv_ctx):
        return get_rpc_consumer(srv_ctx, self._consumer_cls)

    def start(self, srv_ctx):
        rpc_consumer = self.get_consumer(srv_ctx)
        rpc_consumer.register_provider(self)
        rpc_consumer.prepare_queue()

    def on_container_started(self, srv_ctx):
        rpc_consumer = self.get_consumer(srv_ctx)
        rpc_consumer.start()

    def stop(self, srv_ctx):
        rpc_consumer = self.get_consumer(srv_ctx)
        rpc_consumer.unregister_provider(self)
        rpc_consumer.stop()

    def handle_message(self, srv_ctx, body, message):
        args = body['args']
        kwargs = body['kwargs']

        worker_ctx_cls = srv_ctx.container.worker_ctx_cls
        context_data = self.unpack_message_headers(worker_ctx_cls, message)

        handle_result = partial(self.handle_result, message)
        srv_ctx.container.spawn_worker(self, args, kwargs,
                                       context_data=context_data,
                                       handle_result=handle_result)

    def handle_result(self, message, worker_ctx, result, exc):
        srv_ctx = worker_ctx.srv_ctx
        rpc_consumer = self.get_consumer(srv_ctx)
        rpc_consumer.handle_result(message, srv_ctx, result, exc)


class Responder(object):
    def __init__(self, message, retry=True, retry_policy=None):
        self._connection = None
        self.message = message
        self.retry = retry
        if retry_policy is None:
            retry_policy = {'max_retries': 3}
        self.retry_policy = retry_policy

    def connection_factory(self, srv_ctx):
        return Connection(srv_ctx.config[AMQP_URI_CONFIG_KEY])

    def send_response(self, srv_ctx, result, error_wrapper):

        # TODO: if we use error codes outside the payload we would only
        # need to serialize the actual value
        # assumes result is json-serializable
        error = None
        if error_wrapper is not None:
            error = error_wrapper.serialize()

        with self.connection_factory(srv_ctx) as conn:

            with producers[conn].acquire(block=True) as producer:

                reply_to = self.message.properties['reply_to']
                correlation_id = self.message.properties.get('correlation_id')

                msg = {'result': result, 'error': error}

                # all queues are bound to the anonymous direct exchange
                producer.publish(
                    msg, retry=self.retry, retry_policy=self.retry_policy,
                    routing_key=reply_to, correlation_id=correlation_id)


class Service(AttributeDependency):

    def __init__(self, service_name):
        self.service_name = service_name

    def acquire_injection(self, worker_ctx):
        return ServiceProxy(worker_ctx, self.service_name)


class ServiceProxy(object):
    def __init__(self, worker_ctx, service_name):
        self.worker_ctx = worker_ctx
        self.service_name = service_name

    def __getattr__(self, name):
        return MethodProxy(self.worker_ctx, self.service_name, name)


class MethodProxy(HeaderEncoder):

    def __init__(self, worker_ctx, service_name, method_name):
        self.worker_ctx = worker_ctx
        self.service_name = service_name
        self.method_name = method_name

    def __call__(self, *args, **kwargs):
        _log.debug('invoking %s', self)

        worker_ctx = self.worker_ctx
        srv_ctx = worker_ctx.srv_ctx

        msg = {'args': args, 'kwargs': kwargs}

        conn = Connection(srv_ctx.config[AMQP_URI_CONFIG_KEY])
        routing_key = '{}.{}'.format(self.service_name, self.method_name)

        # TODO: should connection sharing be done during call_setup in the
        #       dependency provider?
        with conn as conn:
            reply_queue_name = RPC_REPLY_QUEUE_TEMPLATE.format(
                routing_key, uuid.uuid4())

            exchange = get_rpc_exchange(srv_ctx)
            reply_queue = Queue(
                reply_queue_name, exchange=exchange, exclusive=True)
            maybe_declare(reply_queue, conn)

            with producers[conn].acquire(block=True) as producer:
                # TODO: should we enable auto-retry,
                #      should that be an option in __init__?

                headers = self.get_message_headers(worker_ctx)

                # TODO: should use correlation-id property and check after
                # receiving a response
                producer.publish(msg,
                                 exchange=exchange,
                                 routing_key=routing_key,
                                 reply_to=reply_queue.name,
                                 headers=headers)

            resp_messages = itermessages(
                conn, conn.channel(), reply_queue)

            resp_body, _ = next(resp_messages)

            error = resp_body.get('error')
            if error:
                raise RemoteErrorWrapper.deserialize(error)
            return resp_body['result']

    def __str__(self):
        return '<proxy method: %s.%s>' % (self.service_name, self.method_name)
