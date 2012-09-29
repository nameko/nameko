from kombu import Producer

from newrpc import consuming
from newrpc import entities
from newrpc import responses
from newrpc.channelhandler import ChannelHandler
from newrpc.common import UIDGEN
from newrpc.decorators import ensure

__all__ = ['send_direct', 'send_topic', 'send_fanout', 'send_rpc', ]

DEFAULT_RPC_TIMEOUT = 10


def create_rpcpayload(context, method, args, msg_id=None):
    message = {'method': method, 'args': args, }
    message = context.add_to_message(message)
    if msg_id is None:
        msg_id = UIDGEN()
    if msg_id is not False:
        message['_msg_id'] = msg_id
    return msg_id, message


def send_direct(connection, directid, data):
    exchange = entities.get_reply_exchange(directid)
    with ChannelHandler(connection) as ch:
        producer = Producer(ch.channel,
                exchange=exchange,
                routing_key=directid)
        ch.ensure(producer.publish)(data, declare=[exchange])


def send_topic(connection, exchange, topic, data):
    exchange = entities.get_topic_exchange(exchange)
    with ChannelHandler(connection) as ch:
        producer = Producer(ch.channel,
                exchange=exchange,
                routing_key=topic)
        ch.ensure(producer.publish)(data, declare=[exchange])


def send_fanout(connection, topic, data):
    exchange = entities.get_fanout_exchange(topic)
    with ChannelHandler(connection) as ch:
        producer = Producer(ch.channel,
                exchange=exchange,
                routing_key=topic)
        ch.ensure(producer.publish)(data, declare=[exchange])


@ensure
def send_rpc(connection, context, exchange, topic, method, args,
        timeout=DEFAULT_RPC_TIMEOUT, noreply=False, fanout=False):
    msgid, payload = create_rpcpayload(context, method, args)
    if fanout:
        send_fanout(connection, topic, payload)
        return
    if noreply:
        send_topic(connection, exchange, topic, payload)
        return
    with connection.channel() as channel:
        queue = entities.get_reply_queue(msgid, channel=channel)
        queue.declare()
        send_topic(connection, exchange, topic, payload)
        iter_ = consuming.queue_iterator(queue, timeout=timeout)
        iter_ = responses.iter_rpcresponses(iter_)
        return responses.last(iter_, ack_all=True)


def reply(connection, msg_id, replydata=None, failure=None, on_return=None):
    if on_return is not None:
        raise NotImplementedError('on_return is not implemented')

    msg = {'result': replydata, 'failure': failure, 'ending': False, }
    send_direct(connection, msg_id, msg)
    msg = {'result': None, 'failure': None, 'ending': True, }
    send_direct(connection, msg_id, msg)
