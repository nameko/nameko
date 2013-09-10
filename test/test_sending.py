# TODO: close channels
import uuid

import eventlet

from kombu import Producer

from nameko import context
from nameko import entities
from nameko import sending
from nameko.consuming import queue_iterator
from nameko.responses import ifirst


def reply(connection, msg_id, replydata=None, failure=None):
    msg = {'result': replydata, 'failure': failure, 'ending': False, }
    sending.send_direct(connection, msg_id, msg)
    msg = {'result': None, 'failure': None, 'ending': True, }
    sending.send_direct(connection, msg_id, msg)


def test_send_direct(connection):
    with connection as conn:
        msgid = uuid.uuid4().hex
        with conn.channel() as chan:
            queue = entities.get_reply_queue(msgid=msgid, channel=chan)
            queue.declare()

            sending.send_direct(conn, directid=msgid, data='success')
            msg = ifirst(queue_iterator(queue, no_ack=True, timeout=0.2))
            assert msg.payload == 'success'


def test_send_topic(connection):
    with connection as conn:
        with conn.channel() as chan:
            queue = entities.get_topic_queue('test_rpc', 'test', channel=chan)
            queue.declare()

            sending.send_topic(
                conn,
                exchange='test_rpc',
                topic='test',
                data='success')
            msg = ifirst(queue_iterator(queue, no_ack=True, timeout=0.2))
            assert msg.payload == 'success'


def test_send_rpc(get_connection):
    def response_greenthread():
        with get_connection() as conn:
            with conn.channel() as chan:
                queue = entities.get_topic_queue(
                    'test_rpc', 'test', channel=chan)
                queue.declare()
                msg = ifirst(queue_iterator(queue, no_ack=True, timeout=2))
                msgid, ctx, method, args = context.parse_message(msg.payload)
                reply(conn, msgid, args)

    g = eventlet.spawn_n(response_greenthread)
    eventlet.sleep(0)

    with get_connection() as conn:
        ctx = context.get_admin_context()
        resp = sending.send_rpc(
            conn,
            context=ctx,
            exchange='test_rpc',
            topic='test',
            method='test_method',
            args={'foo': 'bar', },
            timeout=3)

        assert resp == {'foo': 'bar', }

    assert not g


def test_send_rpc_multi_message_reply_ignores_all_but_last(get_connection):
    def response_greenthread():
        with get_connection() as conn:
            with conn.channel() as chan:
                queue = entities.get_topic_queue(
                    'test_rpc', 'test', channel=chan)
                queue.declare()

                msg = ifirst(queue_iterator(queue, no_ack=True, timeout=2))
                msgid, ctx, method, args = context.parse_message(msg.payload)

                exchange = entities.get_reply_exchange(msgid)
                producer = Producer(chan, exchange=exchange, routing_key=msgid)

                for _ in range(3):
                    msg = dict(
                        result='should ignore this message',
                        failure=None, ending=False)
                    producer.publish(msg)
                    eventlet.sleep(0.1)

                msg = dict(result=args, failure=None, ending=False)
                producer.publish(msg)
                msg = dict(result=None, failure=None, ending=True)
                producer.publish(msg)

    g = eventlet.spawn_n(response_greenthread)
    eventlet.sleep()

    with get_connection() as conn:
        ctx = context.get_admin_context()
        resp = sending.send_rpc(
            conn,
            context=ctx,
            exchange='test_rpc',
            topic='test',
            method='test_method',
            args={'spam': 'shrub', },
            timeout=3)

        assert resp == {'spam': 'shrub', }
    eventlet.sleep()
    assert not g
