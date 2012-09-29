# TODO: close channels
import uuid

from newrpc import memory
from newrpc import consuming
from newrpc import context
from newrpc import entities
from newrpc import responses
from newrpc import sending

memory.patch()

import eventlet.debug
eventlet.debug.hub_blocking_detection(True)

ifirst = responses.ifirst
first = responses.first


def test_replying(connection):
    with connection as conn:
        msgid = uuid.uuid4().hex
        with conn.channel() as chan:
            queue = entities.get_reply_queue(msgid=msgid,
                    channel=chan)
            queue.declare()

            sending.reply(conn, msgid, 'success')
            msg = ifirst(consuming.queue_iterator(queue, no_ack=True, timeout=0.2))
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
            msg = ifirst(consuming.queue_iterator(queue, no_ack=True, timeout=0.2))
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
            msg = ifirst(consuming.queue_iterator(queue, no_ack=True, timeout=0.2))
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
            msg = ifirst(consuming.queue_iterator(queue, no_ack=True, timeout=0.2))
            assert msg.payload == 'success'

    # check consumefrom has removed entry
    assert not consuming._conndrainers


def test_send_rpc(get_connection):
    def response_greenthread():
        with get_connection() as conn:
            with conn.channel() as chan:
                queue = entities.get_topic_queue('test_rpc', 'test', channel=chan)
                queue.declare()
                msg = ifirst(consuming.queue_iterator(queue, no_ack=True, timeout=2))
                msgid, ctx, method, args = context.parse_message(msg.payload)
                sending.reply(conn, msgid, args)

    g = eventlet.spawn_n(response_greenthread)
    eventlet.sleep(0)

    with get_connection() as conn:
        ctx = context.get_admin_context()
        resp = sending.send_rpc(conn,
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
