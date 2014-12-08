from concurrent.futures import Future
import eventlet
from eventlet.event import Event
from kombu import Producer
import mock
import pytest

from nameko.exceptions import RemoteError, UnknownService
from nameko.legacy import context
from nameko.legacy import nova
from nameko.legacy.consuming import queue_iterator
from nameko.legacy.proxy import future_rpc
from nameko.legacy.responses import ifirst
from nameko.testing.utils import assert_stops_raising


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

    with mock.patch(
        'nameko.legacy.nova.begin_send_rpc', autospec=True
    ) as begin_send_rpc:
        nova.call(
            connection=conn, context=ctx, topic=topic,
            msg=msg, timeout=timeout, options=options)

        begin_send_rpc.assert_called_with(
            conn, context=ctx, exchange=exchange,
            topic=topic, method=method, args=args,
            timeout=timeout)


def test_delegation_to_send_rpc_default_exchange():

    conn = 'connection'
    ctx = 'context'
    topic = 'topic'
    method = 'foobar'
    args = 'args'
    msg = dict(method=method, args=args)
    timeout = 123
    exchange = 'rpc'

    with mock.patch(
        'nameko.legacy.nova.begin_send_rpc', autospec=True
    ) as begin_send_rpc:
        nova.call(
            connection=conn, context=ctx, topic=topic,
            msg=msg, timeout=timeout)

        begin_send_rpc.assert_called_with(
            conn, context=ctx, exchange=exchange,
            topic=topic, method=method, args=args,
            timeout=timeout)


def test_send_rpc(get_connection):

    queue_declared = Event()

    def response_greenthread():
        with get_connection() as conn:
            with conn.channel() as chan:
                queue = nova.get_topic_queue(
                    'test_rpc', 'test', channel=chan)
                queue.declare()
                queue_declared.send(True)
                msg = ifirst(queue_iterator(queue, no_ack=True, timeout=2))
                msgid, _, _, args = nova.parse_message(msg.payload)

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

        queue_declared.wait()
        resp = nova.send_rpc(
            conn,
            context=ctx,
            exchange='test_rpc',
            topic='test',
            method='test_method',
            args={'foo': 'bar', },
            timeout=3)

        assert resp == {'foo': 'bar', }

    def check_greenthread_dead():
        assert not g
    assert_stops_raising(check_greenthread_dead)


def test_send_rpc_unknown_service(get_connection):
    with get_connection() as conn:
        ctx = context.get_admin_context()

        with pytest.raises(UnknownService):
            nova.send_rpc(
                conn,
                context=ctx,
                exchange='test_rpc',
                topic='test',
                method='test_method',
                args={'foo': 'bar', },
                timeout=3)


def test_send_rpc_errors(get_connection):

    queue_declared = Event()

    def response_greenthread():
        with get_connection() as conn:
            with conn.channel() as chan:
                queue = nova.get_topic_queue(
                    'test_rpc', 'test', channel=chan)
                queue.declare()
                queue_declared.send(True)
                msg = ifirst(queue_iterator(queue, no_ack=True, timeout=2))
                msgid, _, _, _ = nova.parse_message(msg.payload)

                exchange = nova.get_reply_exchange(msgid)
                producer = Producer(chan, exchange=exchange, routing_key=msgid)

                exc = Exception('error')
                failure = (type(exc).__name__, str(exc))

                msg = {'result': None, 'failure': failure, 'ending': False}
                producer.publish(msg)
                msg = {'result': None, 'failure': None, 'ending': True}
                producer.publish(msg)

    g = eventlet.spawn_n(response_greenthread)
    eventlet.sleep(0)

    with get_connection() as conn:
        ctx = context.get_admin_context()

        with pytest.raises(RemoteError):

            queue_declared.wait()
            nova.send_rpc(
                conn,
                context=ctx,
                exchange='test_rpc',
                topic='test',
                method='test_method',
                args={'foo': 'bar', },
                timeout=3)

    def check_greenthread_dead():
        assert not g
    assert_stops_raising(check_greenthread_dead)


def test_send_rpc_multi_message_reply_ignores_all_but_last(get_connection):

    queue_declared = Event()

    def response_greenthread():
        with get_connection() as conn:
            with conn.channel() as chan:
                queue = nova.get_topic_queue(
                    'test_rpc', 'test', channel=chan)
                queue.declare()
                queue_declared.send(True)

                msg = ifirst(queue_iterator(queue, no_ack=True, timeout=2))
                msgid, _, _, args = nova.parse_message(msg.payload)

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

        queue_declared.wait()
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

    def check_greenthread_dead():
        assert not g
    assert_stops_raising(check_greenthread_dead)


def test_send_lazy_rpc(rabbit_config):
    uri = rabbit_config['AMQP_URI']
    with mock.patch(
        'nameko.legacy.nova.begin_send_rpc', autospec=True
    ) as begin_send_rpc:
        response_waiter = mock.Mock()
        begin_send_rpc.return_value = response_waiter
        response_waiter.response.return_value = 'result'

        with future_rpc(uri=uri) as proxy:
            future = proxy.topic.method(foo='bar')
            assert isinstance(future, Future)
            assert not response_waiter.response.called

            res = future.result()
            assert res == 'result'
            assert response_waiter.response.called

        with pytest.raises(RuntimeError):
            proxy.topic.method()


def test_send_lazy_rpc_auto_close(rabbit_config):
    uri = rabbit_config['AMQP_URI']
    with mock.patch(
        'nameko.legacy.nova.begin_send_rpc', autospec=True
    ) as begin_send_rpc:
        response_waiter = mock.Mock()
        begin_send_rpc.return_value = response_waiter
        response_waiter.response.return_value = 'result'

        with future_rpc(uri=uri) as proxy:
            future = proxy.topic.method(foo='bar')
            assert isinstance(future, Future)
            assert not response_waiter.response.called

        # Leaving the executor causes all threads to be evaluated
        assert response_waiter.response.called
        res = future.result()
        assert res == 'result'


def test_send_lazy_rpc_with_exception(rabbit_config):
    uri = rabbit_config['AMQP_URI']
    with mock.patch(
        'nameko.legacy.nova.begin_send_rpc', autospec=True
    ) as begin_send_rpc:
        response_waiter = mock.Mock()
        begin_send_rpc.return_value = response_waiter
        remote_exception = ValueError('RPC method raised exception')
        response_waiter.response.side_effect = remote_exception

        with future_rpc(uri=uri) as proxy:
            future = proxy.topic.method(foo='bar')
            assert isinstance(future, Future)
            assert not response_waiter.response.called

            with pytest.raises(ValueError) as exc_info:
                future.result()

            assert exc_info.value is remote_exception
            assert response_waiter.response.called
