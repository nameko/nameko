from __future__ import absolute_import
from functools import partial
from logging import getLogger
import uuid

from kombu import Connection, Exchange, Queue
from kombu.pools import producers
from kombu.common import itermessages, maybe_declare

from nameko.messaging import ConsumeProvider
from nameko.dependencies import dependency_decorator, AttributeDependency


_log = getLogger(__name__)


@dependency_decorator
def rpc():
    return RpcProvider()

RPC_EXCHANGE = Exchange('nameko-rpc', durable=True)
RPC_QUEUE_TEMPLATE = 'rpc-{}.{}'


class RpcProvider(ConsumeProvider):
    def __init__(self):
        super(RpcProvider, self).__init__(queue=None, requeue_on_error=False)

    def start(self, srv_ctx):
        queue_name = RPC_QUEUE_TEMPLATE.format(srv_ctx.name, self.name)
        routing_key = '{}.{}'.format(srv_ctx.name, self.name)

        self.queue = Queue(
            queue_name,
            exchange=RPC_EXCHANGE,
            routing_key=routing_key,
            durable=True)

        super(RpcProvider, self).start(srv_ctx)

    def handle_message(self, srv_ctx, body, message):
        args = body['args']
        kwargs = body['kwargs']

        reply_to = message.properties['reply_to']
        responder = Responder(reply_to)

        srv_ctx.container.spawn_worker(
            self, args, kwargs,
            handle_result=partial(self.handle_result, message, responder))

    def handle_result(self, message, responder, worker_ctx, result, exc):
        responder.send_response(worker_ctx, result, exc)
        super(RpcProvider, self).handle_result(
            message, worker_ctx, result, exc)


class Responder(object):
    def __init__(self, reply_to):
        self.reply_to = reply_to

    def send_response(self, worker_ctx, result, exc):
        srv_ctx = worker_ctx.srv_ctx

        conn = Connection(srv_ctx.config['amqp_uri'])

        # TODO: if we use error codes outside the payload we would only
        # need to serialize the actual value
        msg = {'result': result, 'error': exc}

        with producers[conn].acquire(block=True) as producer:
            # TODO: should we enable auto-retry,
            #      should that be an option in __init__?

            # all queues are bound to the anonymous direct exchange
            producer.publish(msg, routing_key=self.reply_to)


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

            # TODO: need to do error handling
            return resp_body['result']

    def __str__(self):
        return '<proxy method: %s.%s>' % (self.service_name, self.method_name)
