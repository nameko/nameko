import eventlet
from kombu import Producer

from newrpc import entities
from newrpc import sending
from newrpc.common import UIDGEN
from newrpc.context import add_context_to_payload
from newrpc.consuming import consumefrom
from newrpc.decorators import ensure
from newrpc.exceptions import WaiterTimeout, RemoteError

DEFAULT_RPC_TIMEOUT = 10


def ifirst(iter_, ack=False):
    for i in iter_:
        if ack:
            i.ack()
        return i


def first(iter_, ack_all=False, ack_others=True):
    ret = ifirst(iter_, ack=ack_all)
    for i in iter_:
        if ack_others:
            i.ack()
    return ret


def last(iter_, ack_all=False, ack_others=True):
    i = None
    prev = None
    for i in iter_:
        if ack_others and prev is not None:
            prev.ack()
        prev = i
    if ack_all:
        i.ack()
    return i


def create_rpcpayload(context, method, args, msg_id=None):
    message = {'method': method, 'args': args, }
    message = add_context_to_payload(context, message)
    if msg_id is None:
        msg_id = UIDGEN()
    if msg_id is not False:
        message['_msg_id'] = msg_id
    return msg_id, message


def queue_waiter(queue, channel=None, no_ack=False, timeout=None):
    if queue.is_bound:
        if channel is not None:
            raise TypeError('channel specified when queue is bound')
        channel = queue.channel
    elif channel is not None:
        queue.bind(channel)
    else:
        raise TypeError('channel can not be None for unbound queue')
    channel = queue.channel
    buf = []

    def callback(message):
        try:
            message = channel.message_to_python(message)
        except AttributeError:
            pass
        buf.append(message)

    tag = queue.consume(callback=callback, no_ack=no_ack)
    with eventlet.Timeout(timeout, exception=WaiterTimeout()):
        try:
            while True:
                if buf:
                    yield buf.pop(0)
                consumefrom(channel.connection.client)
        finally:
            queue.cancel(tag)


def iter_rpcresponses(queue, channel=None, timeout=None, **kwargs):
    qw = queue_waiter(queue, channel=channel, timeout=timeout, **kwargs)
    for msg in qw:
        data = msg.payload
        if data['failure']:
            raise RemoteError(**data['failure'])
        elif data.get('ending', False):
            msg.ack()
            return
        else:
            yield msg


@ensure
def send_rpc(connection, context, exchange, topic, method, args,
        timeout=DEFAULT_RPC_TIMEOUT):
    msgid, payload = create_rpcpayload(context, method, args)
    with connection.channel() as channel:
        queue = entities.get_reply_queue(msgid, channel=channel)
        queue.declare()
        sending.send_topic(connection, exchange, topic, payload)
        return last(iter_rpcresponses(queue, timeout=timeout), ack_all=True)


@ensure
def reply(connection, msg_id, replydata=None, failure=None, on_return=None):
    if on_return is not None:
        raise NotImplementedError('on_return is not implemented')

    producer_kwargs = {}
    publish_kwargs = {}
    with connection.channel() as channel:
        msg = {'result': replydata, 'failure': failure, 'ending': False, }
        exchange = entities.get_reply_exchange(msg_id)
        producer = Producer(channel,
                exchange=exchange,
                routing_key=msg_id,
                on_return=on_return,
                **producer_kwargs)
        producer.declare()
        producer.publish(msg, **publish_kwargs)
        msg = {'result': None, 'failure': None, 'ending': True, }
        producer.publish(msg, **publish_kwargs)
