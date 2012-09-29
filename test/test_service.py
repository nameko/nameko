import eventlet
import mock

from newrpc import context
from newrpc import sending
from newrpc import service


def test_service(connection, get_connection):
    m = mock.Mock()
    m().test_method.side_effect = lambda context, **kwargs: kwargs

    srv = service.Service(m, connection=connection,
        exchange='testrpc', topic='test', )
    srv.start()
    eventlet.sleep(0)

    print srv._consumers
    try:
        ctx = context.get_admin_context()
        with get_connection() as conn:
            ret = sending.send_rpc(conn, ctx,
                'testrpc', 'test', 'test_method', args={'foo': 'bar', },
                timeout=2)
            assert ret == {'foo': 'bar', }
    finally:
        srv.kill()
        connection.release()
