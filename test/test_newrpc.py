# TODO: close channels
import uuid

import eventlet
eventlet.monkey_patch()

from kombu import BrokerConnection

import newrpc
from newrpc import memory
from newrpc import consuming
from newrpc import context
from newrpc import entities
from newrpc import sending

memory.patch()

import eventlet.debug
eventlet.debug.hub_blocking_detection(True)

ifirst = newrpc.ifirst
first = newrpc.first


def test_replying(connection):
    with connection as conn:
        msgid = uuid.uuid4().hex
        with conn.channel() as chan:
            queue = entities.get_reply_queue(msgid=msgid,
                    channel=chan)
            queue.declare()

            newrpc.reply(conn, msgid, 'success')
            msg = ifirst(newrpc.queue_iterator(queue, no_ack=True, timeout=0.2))
            assert msg.payload['result'] == 'success'

    # check consumefrom has removed entry
    assert not consuming._conndrainers


def test_send_direct(connection):
    with connection as conn:
        msgid = uuid.uuid4().hex
        with conn.channel() as chan:
            queue = entities.get_reply_queue(msgid=msgid,
                    channel=chan)
            queue.declare()

            sending.send_direct(conn,
                    directid=msgid,
                    data='success')
            msg = ifirst(newrpc.queue_iterator(queue, no_ack=True, timeout=0.2))
            assert msg.payload == 'success'

    # check consumefrom has removed entry
    assert not consuming._conndrainers


def test_send_topic(connection):
    with connection as conn:
        with conn.channel() as chan:
            queue = entities.get_topic_queue('test_rpc', 'test', channel=chan)
            queue.declare()

            sending.send_topic(conn,
                    exchange='test_rpc',
                    topic='test',
                    data='success')
            msg = ifirst(newrpc.queue_iterator(queue, no_ack=True, timeout=0.2))
            assert msg.payload == 'success'

    # check consumefrom has removed entry
    assert not consuming._conndrainers


def test_send_fanout(connection):
    with connection as conn:
        with conn.channel() as chan:
            queue = entities.get_fanout_queue('test', channel=chan)
            queue.declare()

            sending.send_fanout(conn,
                    topic='test',
                    data='success')
            msg = ifirst(newrpc.queue_iterator(queue, no_ack=True, timeout=0.2))
            assert msg.payload == 'success'

    # check consumefrom has removed entry
    assert not consuming._conndrainers


def test_send_rpc(get_connection):
    def response_greenthread():
        with get_connection() as conn:
            with conn.channel() as chan:
                queue = entities.get_topic_queue('test_rpc', 'test', channel=chan)
                queue.declare()
                msg = ifirst(newrpc.queue_iterator(queue, no_ack=True, timeout=2))
                msgid, ctx, method, args = context.parse_message(msg.payload)
                newrpc.reply(conn, msgid, args)

    g = eventlet.spawn_n(response_greenthread)
    eventlet.sleep(0)

    with get_connection() as conn:
        with conn.channel() as chan:
            ctx = context.get_admin_context()
            resp = newrpc.send_rpc(conn,
                    context=ctx,
                    exchange='test_rpc',
                    topic='test',
                    method='test_method',
                    args={'foo': 'bar', },
                    timeout=3)
            #resp.ack()
            assert resp.payload['result'] == {'foo': 'bar', }

    assert not g
    # check consumefrom has removed entry
    assert not consuming._conndrainers
