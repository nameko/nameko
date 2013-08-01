from __future__ import absolute_import
from logging import getLogger
import sys
import traceback

from kombu import Producer

from nameko import consuming
from nameko import context
from nameko import entities
from nameko import exceptions
from nameko import responses

from nameko.channelhandler import ChannelHandler
from nameko.common import UIDGEN
from nameko.decorators import ensure
from nameko.logging import log_time

_log = getLogger(__name__)


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


def send_fanout(connection, topic, data):
    exchange = entities.get_fanout_exchange(topic)
    with ChannelHandler(connection) as ch:
        producer = Producer(
            ch.channel,
            exchange=exchange,
            routing_key=topic)
        ch.ensure(producer.publish)(data, declare=[exchange])


@ensure
def send_rpc(connection, context, exchange, topic, method, args,
             timeout=None, noreply=False, fanout=False):

    msgid = (False if noreply or fanout else None)
    msgid, payload = create_rpcpayload(context, method, args, msg_id=msgid)

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
        ret = responses.last(iter_)
        if ret is not None:
            return ret.payload['result']


def reply(connection, msg_id, replydata=None, failure=None, on_return=None):

    _log.debug('replying to RPC message `%s`', msg_id)

    with log_time(
            _log.debug, 'replied to message `%s` in %0.3f sec.', msg_id):
        if on_return is not None:
            raise NotImplementedError('on_return is not implemented')

        msg = {'result': replydata, 'failure': failure, 'ending': False, }
        send_direct(connection, msg_id, msg)
        msg = {'result': None, 'failure': None, 'ending': True, }
        send_direct(connection, msg_id, msg)


def _delegate_apply(delegate, context, method, args):
    try:
        func = getattr(delegate, method)
    except AttributeError:
        raise exceptions.MethodNotFound(method)
    return func(context=context, **args)


def process_rpc_message(connection, delegate, body):
    msgid, ctx, method, args = context.parse_message(body)

    _log.debug('processing RPC message `%s`: using %s(...)', msgid, method)

    with log_time(
            _log.debug, 'processed RPC message `%s` in %0.3f sec.', msgid):
        try:
            ret = _delegate_apply(delegate, ctx, method, args)
        except Exception:
            exc_typ, exc_val, exc_tb = sys.exc_info()
            if msgid:
                tbfmt = traceback.format_exception(exc_typ, exc_val, exc_tb)
                tbfmt = ''.join(tbfmt)
                ret = (exc_typ.__name__, str(exc_val), tbfmt)
                reply(connection, msgid, failure=ret)
        else:
            if msgid:
                reply(connection, msgid, replydata=ret)
