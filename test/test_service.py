import eventlet
import mock
import pytest

from newrpc import context
from newrpc import exceptions
from newrpc import sending
from newrpc import service


def test_service(connection, get_connection):
    m = mock.Mock()
    m().test_method.side_effect = lambda context, **kwargs: kwargs

    srv = service.Service(m, connection=connection,
        exchange='testrpc', topic='test', )
    srv.start()
    eventlet.sleep(0)

    try:
        ctx = context.get_admin_context()
        with get_connection() as conn:
            ret = sending.send_rpc(conn, ctx,
                'testrpc', 'test', 'test_method', args={'foo': 'bar', },
                timeout=3)
            assert m().test_method.called
            assert ret == {'foo': 'bar', }
    finally:
        srv.kill()
        connection.release()


def test_exceptions(connection, get_connection):
    class Controller(object):
        def test_method(self, context, **kwargs):
            raise KeyError('foo')

    srv = service.Service(Controller,
            connection=connection,
            exchange='testrpc', topic='test', )
    srv.start()
    eventlet.sleep(0)

    try:
        ctx = context.get_admin_context()
        with get_connection() as conn:
            with pytest.raises(exceptions.RemoteError):
                sending.send_rpc(conn, ctx,
                        'testrpc', 'test', 'test_method',
                        args={})
    finally:
        srv.kill()
        connection.release()
