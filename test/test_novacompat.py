import eventlet
eventlet.monkey_patch()

import pytest
try:
    pytest.importorskip('nova')
    from nova import context as novacontext
    from nova import flags
    from nova import rpc
    from nova import version as novaversion
    novaversion   # pyflakes
except:
    raise

from newrpc import context
from newrpc import consuming
from newrpc import entities
from newrpc import responses
from newrpc import sending
from newrpc import service

essexonly = pytest.mark.skipif("'2012.1' >"
        " novaversion.canonical_version_string() >= '2012.2'")


def setup_module(module):
    flags.FLAGS.fake_rabbit = True


@essexonly
def test_sending_rpc_call_to_nova(connection):
    class Proxy(object):
        def testmethod(self, context, foo):
            return {'foo': foo, }

    novaconn = rpc.create_connection()
    novaconn.create_consumer('test', Proxy())

    gt = eventlet.spawn(novaconn.consume)
    try:
        eventlet.sleep(0)

        with connection as newconn:
            resp = sending.send_rpc(newconn,
                    context.get_admin_context(),
                    exchange=flags.FLAGS.control_exchange,
                    topic='test',
                    method='testmethod',
                    args={'foo': 'bar', },
                    timeout=2)
            assert resp == {'foo': 'bar', }
    finally:
        gt.kill()
        novaconn.close()


@essexonly
def test_replying_to_nova_call(connection):
    with connection as conn:
        with conn.channel() as chan:
            queue = entities.get_topic_queue(flags.FLAGS.control_exchange,
                    topic='test',
                    channel=chan)
            queue.declare()

            def listen():
                msg = responses.ifirst(consuming.queue_iterator(
                        queue, no_ack=True, timeout=2))
                msg.ack()
                msgid, ctx, method, args = context.parse_message(msg.payload)
                sending.reply(conn, msgid, (method, args))
            eventlet.spawn(listen)

            res = rpc.call(novacontext.get_admin_context(),
                    topic='test',
                    msg={'method': 'testmethod',
                         'args': {'foo': 'bar', }, },
                    timeout=2)
            assert res == ['testmethod', {'foo': 'bar'}]


@essexonly
def test_sending_from_nova_to_service(connection):
    with connection as conn:
        class Controller(object):
            def test_method(self, context, **kwargs):
                return kwargs

        srvobj = service.Service(Controller, conn,
                flags.FLAGS.control_exchange, 'test')
        srvobj.start()
        eventlet.sleep(0)
        try:
            res = rpc.call(novacontext.get_admin_context(),
                    topic='test',
                    msg={'method': 'test_method',
                         'args': {'foo': 'bar', }, },
                    timeout=2)
            assert res == {'foo': 'bar'}
        finally:
            srvobj.kill()
