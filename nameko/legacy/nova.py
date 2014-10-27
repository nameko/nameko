from __future__ import absolute_import
from logging import getLogger

from kombu import Producer, Exchange, Queue

from nameko.constants import DEFAULT_RETRY_POLICY
from nameko.exceptions import UnknownService
from nameko.legacy import consuming, responses
from nameko.legacy.context import Context
from nameko.legacy.channelhandler import ChannelHandler
from nameko.legacy.common import UIDGEN
from nameko.legacy.decorators import ensure


_log = getLogger(__name__)


CONTROL_EXCHANGE = 'rpc'
DEFAULT_RPC_TIMEOUT = 10
DURABLE_QUEUES = False


NO_RESPONSE = object()


def get_reply_exchange(msgid, channel=None):
    return Exchange(
        name=msgid,
        channel=channel,
        type='direct',
        durable=False,
        auto_delete=True)


def get_reply_queue(msgid, channel=None):
    exchange = get_reply_exchange(msgid, channel=channel)
    return Queue(
        name=msgid,
        channel=channel,
        exchange=exchange,
        routing_key=msgid,
        durable=False,
        auto_delete=True,
        exclusive=True)


def get_topic_exchange(exchange_name, channel=None):
    return Exchange(
        name=exchange_name,
        channel=channel,
        type='topic',
        durable=DURABLE_QUEUES,
        auto_delete=False)


def get_topic_queue(exchange_name, topic, channel=None):
    exchange = get_topic_exchange(exchange_name, channel=channel)
    return Queue(
        name=topic,
        channel=channel,
        exchange=exchange,
        routing_key=topic,
        durable=DURABLE_QUEUES,
        auto_delete=False,
        exclusive=False)


def _get_exchange(options):
    if options is not None:
        return options.get('CONTROL_EXCHANGE', CONTROL_EXCHANGE)
    return CONTROL_EXCHANGE


def _create_rpcpayload(context, method, args):
    message = {'method': method, 'args': args, }
    message = context.add_to_message(message)
    msg_id = UIDGEN()
    message['_msg_id'] = msg_id
    return msg_id, message


def _send_topic(connection, exchange, topic, data):
    exchange = get_topic_exchange(exchange)
    with ChannelHandler(connection) as ch:
        producer = Producer(
            ch.channel,
            exchange=exchange,
            routing_key=topic,
            )
        ch.ensure(producer.publish)(data, declare=[exchange], mandatory=True,
                                    retry_policy=DEFAULT_RETRY_POLICY,
                                    retry=True)

        # see comment in rpc.MethodProxy.__call__, after publish()
        if not producer.channel.returned_messages.empty():
            raise UnknownService(topic)


def parse_message(message_body):
    method = message_body.get('method')
    args = message_body.get('args')
    msg_id = message_body.get('_msg_id', None)
    context_dict = dict(
        (k[9:], message_body.get(k))
        for k in message_body.keys() if k.startswith('_context_')
    )
    context = Context(**context_dict)
    return msg_id, context, method, args


class ResponseWaiter(object):
    def __init__(self, queue, timeout, channel, connection):
        self.queue = queue
        self.timeout = timeout
        self.channel = channel
        self.connection = connection

        self._response = NO_RESPONSE

    def response(self):
        if self._response is NO_RESPONSE:
            iter_ = consuming.queue_iterator(self.queue, timeout=self.timeout)
            iter_ = responses.iter_rpcresponses(iter_)
            ret = responses.last(iter_)
            if ret is not None:
                ret = ret.payload['result']

            self._response = ret
            self.channel.__exit__()
            self.connection.close()

        return self._response


@ensure
def begin_send_rpc(
    connection, context, exchange, topic, method, args, timeout=None
):
    _log.info('rpc: %s %s.%s', exchange, topic, method)

    msgid, payload = _create_rpcpayload(context, method, args)

    channel = connection.channel().__enter__()
    queue = get_reply_queue(msgid, channel=channel)
    queue.declare()
    _send_topic(connection, exchange, topic, payload)

    return ResponseWaiter(
        queue, timeout, channel, connection,
    )


@ensure
def send_rpc(connection, context, exchange, topic, method, args, timeout=None):
    waiter = begin_send_rpc(
        connection, context, exchange, topic, method, args, timeout
    )
    return waiter.response()


def begin_call(connection, context, topic, msg,
               timeout=DEFAULT_RPC_TIMEOUT, options=None):
    exchange = _get_exchange(options)
    waiter = begin_send_rpc(
        connection,
        context=context,
        exchange=exchange,
        topic=topic,
        method=msg['method'],
        args=msg['args'],
        timeout=timeout
    )
    return waiter


def call(connection, context, topic, msg,
         timeout=DEFAULT_RPC_TIMEOUT, options=None):
    waiter = begin_call(
        connection, context, topic, msg, timeout=timeout, options=options
    )
    return waiter.response()
