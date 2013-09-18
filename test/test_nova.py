import eventlet
from kombu import Producer
import mock

from nameko import context
from nameko import nova
from nameko.consuming import queue_iterator
from nameko.responses import ifirst


def test_delegation_to_send_rpc():

    conn = 'connection'
    ctx = 'context'
    topic = 'topic'
    method = 'foobar'
    args = 'args'
    msg = dict(method=method, args=args)
    timeout = 123
    exchange = 'spam_exchange'
    options = {'CONTROL_EXCHANGE': exchange}

    with mock.patch('nameko.nova.send_rpc') as send_rpc:
        nova.call(
            connection=conn, context=ctx, topic=topic,
            msg=msg, timeout=timeout, options=options)

        send_rpc.assert_called_with(
            conn, context=ctx, exchange=exchange,
            topic=topic, method=method, args=args,
            timeout=timeout)


def test_send_rpc(get_connection):
    def response_greenthread():
        with get_connection() as conn:
            with conn.channel() as chan:
                queue = nova.get_topic_queue(
                    'test_rpc', 'test', channel=chan)
                queue.declare()
                msg = ifirst(queue_iterator(queue, no_ack=True, timeout=2))
                msgid, ctx, method, args = nova.parse_message(msg.payload)

                exchange = nova.get_reply_exchange(msgid)
                producer = Producer(chan, exchange=exchange, routing_key=msgid)

                msg = {'result': args, 'failure': None, 'ending': False}
                producer.publish(msg)
                msg = {'result': None, 'failure': None, 'ending': True}
                producer.publish(msg)

    g = eventlet.spawn_n(response_greenthread)
    eventlet.sleep(0)

    with get_connection() as conn:
        ctx = context.get_admin_context()
        resp = nova.send_rpc(
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
                queue = nova.get_topic_queue(
                    'test_rpc', 'test', channel=chan)
                queue.declare()

                msg = ifirst(queue_iterator(queue, no_ack=True, timeout=2))
                msgid, ctx, method, args = nova.parse_message(msg.payload)

                exchange = nova.get_reply_exchange(msgid)
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
        resp = nova.send_rpc(
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
