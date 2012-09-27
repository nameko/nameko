# TODO: close channels
import uuid

import eventlet
eventlet.monkey_patch()

from kombu import BrokerConnection

import newrpc
from newrpc import memory
from newrpc import consuming

memory.patch()

import eventlet.debug
eventlet.debug.hub_blocking_detection(True)


def get_connection():
    conn = BrokerConnection(transport='memory')
    return conn


def test_replying():
    with get_connection() as conn:
        msgid = uuid.uuid4().hex
        with conn.channel() as chan:
            queue = newrpc.get_reply_queue(msgid=msgid,
                    channel=chan)
            queue.declare()

            with conn.channel() as chan2:
                newrpc.reply(chan2, msgid, 'success')
            msg = newrpc.queue_waiter(queue, no_ack=True, timeout=0.2)
            assert msg.payload['result'] == 'success'

    # check consumefrom has removed entry
    assert not consuming._conndrainers


def test_send_direct():
    with get_connection() as conn:
        msgid = uuid.uuid4().hex
        with conn.channel() as chan:
            queue = newrpc.get_reply_queue(msgid=msgid,
                    channel=chan)
            queue.declare()

            with conn.channel() as chan2:
                newrpc.send_direct(channel=chan2,
                        directid=msgid,
                        data='success')
            msg = newrpc.queue_waiter(queue, no_ack=True, timeout=0.2)
            assert msg.payload == 'success'

    # check consumefrom has removed entry
    assert not consuming._conndrainers


def test_send_topic():
    with get_connection() as conn:
        with conn.channel() as chan:
            queue = newrpc.get_topic_queue('test_rpc', 'test', channel=chan)
            queue.declare()

            with conn.channel() as chan2:
                newrpc.send_topic(channel=chan2,
                        exchange='test_rpc',
                        topic='test',
                        data='success')
            msg = newrpc.queue_waiter(queue, no_ack=True, timeout=0.2)
            assert msg.payload == 'success'

    # check consumefrom has removed entry
    assert not consuming._conndrainers


def test_send_fanout():
    with get_connection() as conn:
        with conn.channel() as chan:
            queue = newrpc.get_fanout_queue('test', channel=chan)
            queue.declare()

            with conn.channel() as chan2:
                newrpc.send_fanout(channel=chan2,
                        topic='test',
                        data='success')
            msg = newrpc.queue_waiter(queue, no_ack=True, timeout=0.2)
            assert msg.payload == 'success'

    # check consumefrom has removed entry
    assert not consuming._conndrainers


def test_send_rpc():
    def response_greenthread():
        with get_connection() as conn:
            with conn.channel() as chan:
                queue = newrpc.get_topic_queue('test_rpc', 'test', channel=chan)
                queue.declare()
                msg = newrpc.queue_waiter(queue, no_ack=True, timeout=2)
                print 'got', msg
                msgid, ctx, method, args = newrpc.parse_message(msg.payload)
                newrpc.reply(chan, msgid, args)

    g = eventlet.spawn_n(response_greenthread)
    eventlet.sleep(0)

    with get_connection() as conn:
        with conn.channel() as chan:
            context = newrpc.get_admin_context()
            resp = newrpc.send_rpc(context,
                    channel=chan,
                    exchange='test_rpc',
                    topic='test',
                    method='test_method',
                    args={'foo': 'bar', },
                    timeout=3)
            resp.ack()
            assert resp.payload['result'] == {'foo': 'bar', }

    assert not g
    # check consumefrom has removed entry
    assert not consuming._conndrainers
