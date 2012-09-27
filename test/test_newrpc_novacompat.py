import eventlet
eventlet.monkey_patch()

import pytest
try:
    pytest.importorskip('nova')
    from nova import context
    from nova import flags
    from nova import rpc
    import nova.version
except:
    raise

import newrpc
from newrpc import memory
memory.patch()

essexonly = pytest.mark.skipif("'2012.1' <="
        " nova.version.canonical_version_string() < '2012.2'")


def setup_module(module):
    flags.FLAGS.fake_rabbit = True


@essexonly
def test_sending_rpc_call_to_nova(connection):
    class Proxy(object):
        def testmethod(self, context, foo):
            print 'called'
            return {'foo': foo, }

    novaconn = rpc.create_connection()
    novaconn.create_consumer('test', Proxy())

    gt = eventlet.spawn(novaconn.consume)
    try:
        eventlet.sleep(0)

        with connection as newconn:
            with newconn.channel() as chan:
                resp = newrpc.send_rpc(newrpc.get_admin_context(),
                        channel=chan,
                        exchange=flags.FLAGS.control_exchange,
                        topic='test',
                        method='testmethod',
                        args={'foo': 'bar', },
                        timeout=2)
                resp.ack()
                assert resp.payload['result'] == {'foo': 'bar', }
    finally:
        gt.kill()
        novaconn.close()


@essexonly
def test_replying_to_nova_call(connection):
    with connection as conn:
        with conn.channel() as chan:
            queue = newrpc.get_topic_queue(flags.FLAGS.control_exchange,
                    topic='test',
                    channel=chan)
            queue.declare()

            def listen():
                msg = newrpc.ifirst(newrpc.queue_waiter(queue, no_ack=True, timeout=2))
                msg.ack()
                msgid, ctx, method, args = newrpc.parse_message(msg.payload)
                with conn.channel() as chan2:
                    newrpc.reply(chan2, msgid, (method, args))
            eventlet.spawn(listen)

            res = rpc.call(context.get_admin_context(),
                    topic='test',
                    msg={'method': 'testmethod',
                         'args': {'foo': 'bar', }, },
                    timeout=2)
            assert res == ['testmethod', {'foo': 'bar'}]
