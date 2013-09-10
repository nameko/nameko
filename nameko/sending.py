from __future__ import absolute_import
from logging import getLogger

from kombu import Producer

from nameko import consuming
from nameko import entities
from nameko import responses

from nameko.channelhandler import ChannelHandler
from nameko.common import UIDGEN
from nameko.decorators import ensure

_log = getLogger(__name__)


def create_rpcpayload(context, method, args):
    message = {'method': method, 'args': args, }
    message = context.add_to_message(message)
    msg_id = UIDGEN()
    message['_msg_id'] = msg_id
    return msg_id, message


def send_direct(connection, directid, data):
    exchange = entities.get_reply_exchange(directid)
    with ChannelHandler(connection) as ch:
        producer = Producer(
            ch.channel,
            exchange=exchange,
            routing_key=directid)
        ch.ensure(producer.publish)(data, declare=[exchange])


def send_topic(connection, exchange, topic, data):
    exchange = entities.get_topic_exchange(exchange)
    with ChannelHandler(connection) as ch:
        producer = Producer(
            ch.channel,
            exchange=exchange,
            routing_key=topic)
        ch.ensure(producer.publish)(data, declare=[exchange])


@ensure
def send_rpc(connection, context, exchange, topic, method, args, timeout=None):

    _log.info('rpc: %s %s.%s', exchange, topic, method)

    msgid, payload = create_rpcpayload(context, method, args)

    with connection.channel() as channel:
        queue = entities.get_reply_queue(msgid, channel=channel)
        queue.declare()
        send_topic(connection, exchange, topic, payload)
        iter_ = consuming.queue_iterator(queue, timeout=timeout)
        iter_ = responses.iter_rpcresponses(iter_)
        ret = responses.last(iter_)
        if ret is not None:
            return ret.payload['result']
