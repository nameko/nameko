from kombu import Producer

from newrpc import entities
from newrpc import responses
from newrpc import sending
from newrpc.common import UIDGEN
from newrpc.context import add_context_to_payload
from newrpc.consuming import queue_iterator
from newrpc.decorators import ensure

DEFAULT_RPC_TIMEOUT = 10


def create_rpcpayload(context, method, args, msg_id=None):
    message = {'method': method, 'args': args, }
    message = add_context_to_payload(context, message)
    if msg_id is None:
        msg_id = UIDGEN()
    if msg_id is not False:
        message['_msg_id'] = msg_id
    return msg_id, message


@ensure
def send_rpc(connection, context, exchange, topic, method, args,
        timeout=DEFAULT_RPC_TIMEOUT):
    msgid, payload = create_rpcpayload(context, method, args)
    with connection.channel() as channel:
        queue = entities.get_reply_queue(msgid, channel=channel)
        queue.declare()
        sending.send_topic(connection, exchange, topic, payload)
        iter_ = queue_iterator(queue, timeout=timeout)
        iter_ = responses.iter_rpcresponses(iter_)
        return responses.last(iter_, ack_all=True)


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
