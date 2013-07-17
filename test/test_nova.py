
import mock
import pytest

from nameko import nova


def test_multicall():
    with pytest.raises(NotImplementedError):
        nova.multicall(None, None, None, None)


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

    with mock.patch('nameko.sending.send_rpc') as send_rpc:
        nova.call(
            connection=conn, context=ctx, topic=topic,
            msg=msg, timeout=timeout, options=options)

        send_rpc.assert_called_with(
            conn, context=ctx, exchange=exchange,
            topic=topic, method=method, args=args,
            timeout=timeout)

        nova.cast(
            connection=conn, context=ctx, topic=topic,
            msg=msg, options=options)

        send_rpc.assert_called_with(
            conn, context=ctx, exchange=exchange,
            topic=topic, method=method, args=args,
            noreply=True)

        nova.fanout_cast(connection=conn, context=ctx, topic=topic, msg=msg)

        send_rpc.assert_called_with(
            conn, context=ctx, exchange=None,
            topic=topic, method=method, args=args, fanout=True)
