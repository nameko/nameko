from __future__ import absolute_import
from functools import partial
from logging import getLogger
import uuid
from weakref import WeakKeyDictionary

from kombu import Connection, Exchange, Queue
from kombu.pools import producers
from kombu.common import itermessages, maybe_declare

from nameko.exceptions import MethodNotFound, RemoteErrorWrapper
from nameko.messaging import QueueConsumer
from nameko.dependencies import (
    dependency_decorator, AttributeDependency, DecoratorDependency)


_log = getLogger(__name__)


@dependency_decorator
def rpc():
    return RpcProvider()

RPC_EXCHANGE = Exchange('nameko-rpc', durable=True, type="topic")
RPC_QUEUE_TEMPLATE = 'rpc-{}'


rpc_consumers = WeakKeyDictionary()


def get_rpc_consumer(srv_ctx):
    """ Get or create an RpcConsumer instance for the given ``srv_ctx``
    """
    if srv_ctx not in rpc_consumers:
        rpc_consumer = RpcConsumer(srv_ctx)
        rpc_consumers[srv_ctx] = rpc_consumer

    return rpc_consumers[srv_ctx]


class RpcConsumer(QueueConsumer):

    def __init__(self, srv_ctx):
        self._queue = None
        self._providers = {}
        self._srv_ctx = srv_ctx

        amqp_uri = srv_ctx.config['amqp_uri']
        max_workers = srv_ctx.max_workers
        super(RpcConsumer, self).__init__(amqp_uri, max_workers)

    def prepare_queue(self):
        if self._queue is None:

            service_name = self._srv_ctx.name
            queue_name = RPC_QUEUE_TEMPLATE.format(service_name)
            routing_key = '{}.*'.format(service_name)

            self._queue = Queue(
                queue_name,
                exchange=RPC_EXCHANGE,
                routing_key=routing_key,
                durable=True)

            self.add_consumer(self._queue, self.handle_message)

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

    def handle_message(self, body, message):
        routing_key = message.delivery_info['routing_key']
        try:
            provider = self._providers[routing_key]
        except KeyError:
            self.handle_method_not_found(body, message)
        else:
            srv_ctx = self._srv_ctx
            provider.handle_message(srv_ctx, body, message)

    def handle_method_not_found(self, body, message):
        routing_key = message.delivery_info['routing_key']
        method_name = routing_key.split(".")[-1]

        try:  # raise to generate a sensible traceback
            raise MethodNotFound(method_name)
        except MethodNotFound as exc:
            error = RemoteErrorWrapper(exc)

        reply_to = message.properties['reply_to']
        correlation_id = message.properties.get('correlation_id')
        responder = Responder(reply_to, correlation_id)

        srv_ctx = self._srv_ctx
        responder.send_response(srv_ctx, None, error)
        self.ack_message(message)


class RpcProvider(DecoratorDependency):

    def start(self, srv_ctx):
        rpc_consumer = get_rpc_consumer(srv_ctx)
        rpc_consumer.register_provider(self)
        rpc_consumer.prepare_queue()

    def on_container_started(self, srv_ctx):
        rpc_consumer = get_rpc_consumer(srv_ctx)
        rpc_consumer.register_provider(self)
        rpc_consumer.start()

    def stop(self, srv_ctx):
        rpc_consumer = get_rpc_consumer(srv_ctx)
        rpc_consumer.unregister_provider(self)
        rpc_consumer.stop()  # waits for jobs?

    def handle_message(self, srv_ctx, body, message):
        args = body['args']
        kwargs = body['kwargs']

        reply_to = message.properties['reply_to']
        correlation_id = message.properties.get('correlation_id')

        responder = Responder(reply_to, correlation_id)
        srv_ctx.container.spawn_worker(
            self, args, kwargs,
            handle_result=partial(self.handle_result, message, responder))

    def handle_result(self, message, responder, worker_ctx, result, exc):

        error = None
        if exc is not None:
            error = RemoteErrorWrapper(exc)

        responder.send_response(worker_ctx.srv_ctx, result, error)
        rpc_consumer = get_rpc_consumer(worker_ctx.srv_ctx)
        rpc_consumer.ack_message(message)


class Responder(object):
    def __init__(self, reply_to, correlation_id):
        self.reply_to = reply_to
        self.correlation_id = correlation_id

    def send_response(self, srv_ctx, result, error_wrapper):

        conn = Connection(srv_ctx.config['amqp_uri'])

        # TODO: if we use error codes outside the payload we would only
        # need to serialize the actual value
        # assumes result is json-serializable
        error = None
        if error_wrapper is not None:
            error = error_wrapper.serialize()

        msg = {'result': result, 'error': error}

        with producers[conn].acquire(block=True) as producer:
            # TODO: should we enable auto-retry,
            #      should that be an option in __init__?

            # all queues are bound to the anonymous direct exchange
            producer.publish(msg, routing_key=self.reply_to,
                             correlation_id=self.correlation_id)


class Service(AttributeDependency):
    def __init__(self, service_name):
        self.service_name = service_name

    def acquire_injection(self, worker_ctx):
        return ServiceProxy(self.service_name, worker_ctx)


class ServiceProxy(object):
    def __init__(self, service_name, worker_ctx):
        self.worker_ctx = worker_ctx
        self.service_name = service_name

    def __getattr__(self, name):
        return MethodProxy(self.service_name, name, self.worker_ctx)


class MethodProxy(object):
    def __init__(self, service_name, method_name, worker_ctx):
        self.worker_ctx = worker_ctx
        self.service_name = service_name
        self.method_name = method_name

    def __call__(self, *args, **kwargs):
        _log.debug('invoking %s', self)

        worker_ctx = self.worker_ctx
        srv_ctx = worker_ctx.srv_ctx

        msg = {'args': args, 'kwargs': kwargs}

        conn = Connection(srv_ctx.config['amqp_uri'])
        routing_key = '{}.{}'.format(self.service_name, self.method_name)

        # TODO: should connection sharing be done during call_setup in the
        #       dependency provider?
        with conn as conn:
            reply_queue_name = 'rpc.reply-{}-{}'.format(
                routing_key, uuid.uuid4())

            reply_queue = Queue(
                reply_queue_name, exchange=RPC_EXCHANGE, exclusive=True)
            maybe_declare(reply_queue, conn)

            with producers[conn].acquire(block=True) as producer:
                # TODO: should we enable auto-retry,
                #      should that be an option in __init__?

                # TODO: should use correlation-id property and check after
                # receiving a response
                producer.publish(msg,
                                 exchange=RPC_EXCHANGE,
                                 routing_key=routing_key,
                                 reply_to=reply_queue.name)

            resp_messages = itermessages(
                conn, conn.channel(), reply_queue)

            resp_body, resp_message = next(resp_messages)

            error = resp_body.get('error')
            if error:
                raise RemoteErrorWrapper.deserialize(error)
            return resp_body['result']

    def __str__(self):
        return '<proxy method: %s.%s>' % (self.service_name, self.method_name)
