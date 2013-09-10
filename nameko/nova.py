from __future__ import absolute_import
from functools import partial
from logging import getLogger
import sys
import traceback


from weakref import WeakKeyDictionary

from kombu import Connection
from kombu.pools import producers

from nameko import context
from nameko import sending
from nameko.entities import get_topic_queue
from nameko.exceptions import MethodNotFound
from nameko.messaging import get_queue_consumer
from nameko.dependencies import DecoratorDependency

_log = getLogger(__name__)


CONTROL_EXCHANGE = 'rpc'
DEFAULT_RPC_TIMEOUT = 10


def _get_exchange(options):
    if options is not None:
        return options.get('CONTROL_EXCHANGE', CONTROL_EXCHANGE)
    return CONTROL_EXCHANGE


def call(connection, context, topic, msg,
         timeout=DEFAULT_RPC_TIMEOUT, options=None):
    exchange = _get_exchange(options)
    return sending.send_rpc(
        connection,
        context=context,
        exchange=exchange,
        topic=topic,
        method=msg['method'],
        args=msg['args'],
        timeout=timeout)

RPC_QUEUE_TEMPLATE = '{}'

rpc_consumers = WeakKeyDictionary()


def get_rpc_consumer(srv_ctx):
    """ Get or create an RpcConsumer instance for the given ``srv_ctx``
    """
    if srv_ctx not in rpc_consumers:
        rpc_consumer = RpcConsumer(srv_ctx)
        rpc_consumers[srv_ctx] = rpc_consumer

    return rpc_consumers[srv_ctx]


class RpcConsumer(object):

    def __init__(self, srv_ctx):
        self._queue = None
        self._providers = {}
        self._srv_ctx = srv_ctx

    def prepare_queue(self):
        if self._queue is None:

            service_name = self._srv_ctx.name

            exchange_name = self._srv_ctx.config.get(
                'CONTROL_EXCHANGE', CONTROL_EXCHANGE)

            self._queue = get_topic_queue(exchange_name, service_name)

            srv_ctx = self._srv_ctx
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
        srv_ctx = self._srv_ctx
        try:
            routing_key = '{}.{}'.format(
                message.delivery_info['routing_key'],
                body.get('method'))

            _log.info("handling rpc message: %s", routing_key)

            provider = self.get_provider_for_method(routing_key)

            provider.handle_message(srv_ctx, body, message)
        except Exception as exc:
            msgid = body.get('_msg_id', None)
            self.handle_result(message, msgid, srv_ctx, None, exc)

    def handle_result(self, message, msgid, srv_ctx, result, error):
        responder = Responder(msgid)
        responder.send_response(srv_ctx, result, error)

        qc = get_queue_consumer(srv_ctx)
        qc.ack_message(message)


class NovaRpcProvider(DecoratorDependency):

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
        rpc_consumer.stop()

    def handle_message(self, srv_ctx, body, message):
        msgid, ctx, method, kwargs = context.parse_message(body)
        args = []
        kwargs['context'] = ctx

        handle_result = partial(self.handle_result, message, msgid)

        srv_ctx.container.spawn_worker(self, args, kwargs,
                                       handle_result=handle_result)

    def handle_result(self, message, msgid, worker_ctx, result, exc):
        srv_ctx = worker_ctx.srv_ctx
        rpc_consumer = get_rpc_consumer(srv_ctx)
        rpc_consumer.handle_result(message, msgid, srv_ctx, result, exc)


class Responder(object):
    def __init__(self, msgid):
        self.msgid = msgid

    def send_response(self, srv_ctx, result, exc):
        if not self.msgid:
            return

        if exc is not None:
            exc_typ, exc_val, exc_tb = sys.exc_info()
            tbfmt = traceback.format_exception(
                exc_typ, exc_val, exc_tb)
            tbfmt = ''.join(tbfmt)
            failure = (exc_typ.__name__, str(exc_val), tbfmt)
        else:
            failure = None

        conn = Connection(srv_ctx.config['amqp_uri'])

        with producers[conn].acquire(block=True) as producer:
            messages = [
                {'result': result, 'failure': failure, 'ending': False},
                {'result': None, 'failure': None, 'ending': True},
            ]

            for msg in messages:
                producer.publish(msg, routing_key=self.msgid)
