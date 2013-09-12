import socket

from eventlet.event import Event
from eventlet.greenpool import GreenPool
import eventlet
import pytest

from kombu import Connection

from nameko import exceptions
from nameko import service
from nameko.testing import TestProxy
from nameko.timer import timer


def test_service(get_connection):
    class Controller(object):
        def test_method(self, context, foo):
            return foo + 'spam'

    srv = service.Service(
        Controller, connection_factory=get_connection,
        exchange='testrpc', topic='test', )
    srv.start()
    eventlet.sleep()

    try:
        test = TestProxy(get_connection, timeout=3).test
        ret = test.test_method(foo='bar')

        assert ret == 'barspam'
    finally:
        srv.kill()


def test_service_socket_error():

    class FailConnection(Connection):
        # a connection that will give a socket.error
        # when the service starts
        def drain_events(self, *args, **kwargs):
            raise socket.error

    def get_connection():
        return FailConnection(transport='memory')

    class Controller(object):
        def test_method(self, context, foo):
            return foo + 'spam'

    srv = service.Service(
        Controller, connection_factory=get_connection,
        exchange='testrpc', topic='test', )

    try:
        srv.start()
        eventlet.sleep()
        with pytest.raises(socket.error):
            # we expect that a socket.error has been raised
            # within the startup greenthread
            # calling .wait() should raise this returned error
            srv.greenlet.wait()

    finally:
        srv.kill()


def test_service_doesnt_exhaust_pool(get_connection):
    POOLSIZE = 10

    class Controller(object):
        def test_method(self, context, foo):
            return foo + 'spam'

    srv = service.Service(
        Controller, connection_factory=get_connection,
        exchange='testrpc', topic='test', poolsize=POOLSIZE)
    srv.start()
    eventlet.sleep()

    try:
        with eventlet.Timeout(10):
            for i in range(POOLSIZE * 2):
                test = TestProxy(get_connection, timeout=3).test
                test.test_method(foo='bar')

    finally:
        srv.kill()


def test_exceptions(get_connection):
    class Controller(object):
        def test_method(self, context, **kwargs):
            raise KeyError('foo')

    srv = service.Service(
        Controller, connection_factory=get_connection,
        exchange='testrpc', topic='test', )
    srv.start()
    eventlet.sleep()

    try:
        test = TestProxy(get_connection, timeout=3).test
        with pytest.raises(exceptions.RemoteError):
            test.test_method()

        with pytest.raises(exceptions.RemoteError):
            test.test_method_does_not_exist()
    finally:
        srv.kill()


def test_service_wait(get_connection):
    # TODO: this is a coverage test,
    #       I am not sure the behavior we test is actually what we want.

    spam_continue = Event()
    spam_called = Event()

    class Foobar(object):
        def spam(self, context, **kwargs):
            spam_called.send(1)
            # lets wait until we are told to stop,
            # pretending we are working really hard
            spam_continue.wait()

    srv = service.Service(
        Foobar, connection_factory=get_connection,
        exchange='testrpc', topic='test', )

    srv.start()
    eventlet.sleep()

    try:
        with eventlet.timeout.Timeout(5):
            foobar = TestProxy(get_connection, timeout=3).test
            # lets make sure we actually have a worker currently
            # processing an RPC call
            rpc_call = eventlet.spawn(foobar.spam)
            spam_called.wait()

            # we are in the middle of an RPC call, this will make sure
            # that wait() will have to wait for the worker and the queue
            kill_thread = eventlet.spawn(srv.kill)
            # Let the worker finish it's job
            spam_continue.send(1)
            rpc_call.wait()
            kill_thread.wait()

    except eventlet.timeout.Timeout as t:
        pytest.fail('waiting for death of service timed out: {}'.format(t))
    finally:
        srv.kill()


def test_service_wait_consumer_dies(get_connection):
    srv = service.Service(
        object, connection_factory=get_connection,
        exchange='testrpc', topic='test', )

    srv.start()
    eventlet.sleep()
    srv.greenlet.kill()
    # we don't expect any errors while waiting
    srv.kill()


def test_service_cancells_timers(get_connection):
    timer_evt = Event()

    class Foobar(object):
        @timer(0)
        def shrub(self):
            try:
                timer_evt.send(True)
            except:
                pass

    srv = service.Service(
        Foobar, connection_factory=get_connection,
        exchange='testrpc', topic='test', )

    srv.start()
    # wait for the timer to be called once
    timer_evt.wait()
    # this should stop the timers and we should not get any errors
    srv.kill()
    assert len(srv._timers) == 0


def test_service_link(get_connection):
    srv = service.Service(
        object, connection_factory=get_connection,
        exchange='testrpc', topic='test', )

    srv.start()

    complete_called = []

    def complete(gt, spam, foo):
        assert spam == 'spam'
        assert foo == 'bar'
        assert gt == srv.greenlet
        complete_called.append(True)

    srv.link(complete, 'spam', foo='bar')

    srv.kill()

    assert complete_called == [True]


def test_service_cannot_be_started_twice(get_connection):
    srv = service.Service(
        object, connection_factory=get_connection,
        exchange='testrpc', topic='test')

    srv.start()
    with pytest.raises(RuntimeError):
        srv.start()

    srv.kill()


def test_service_requeues_if_out_of_workers(get_connection):
    foobar_continue = Event()
    foobar_called = Event()

    class Foobar(object):
        def spam(self, context):
            foobar_called.send(True)
            with eventlet.Timeout(2):
                foobar_continue.wait()

    s1 = service.Service(
        Foobar, connection_factory=get_connection,
        exchange='testrpc', topic='test', poolsize=1)

    s1.start()
    eventlet.sleep()

    foobar = TestProxy(get_connection, timeout=3).test

    w1 = eventlet.spawn(foobar.spam)
    foobar_called.wait()
    # the servcie should now be busy and requeue the next request
    w2 = eventlet.spawn(foobar.spam)
    eventlet.sleep()

    # we create a second service to pick up the message
    spam_received = Event()

    class Spam(object):
        def spam(self, context):
            spam_received.send(True)

    s2 = service.Service(
        Spam, connection_factory=get_connection,
        exchange='testrpc', topic='test', poolsize=1)

    s2.start()
    eventlet.sleep()
    # wait for the 2nd service to pick up the message
    with eventlet.Timeout(2):
        assert spam_received.wait()

    # be nice to the first service and tell it to stop being busy
    foobar_continue.send(True)

    w1.wait()
    w2.wait()
    s1.kill()
    s2.kill()


def test_service_custom_pool(get_connection):
    class Foobar(object):
        def spam(self, context, **kwargs):
            pass

    class MyPool(object):
        _pool = GreenPool(size=10)
        count = 0

        def spawn(self, *args, **kwargs):
            self.count += 1
            return self._pool.spawn(*args, **kwargs)

        def waitall(self):
            return self._pool.waitall()

        @property
        def size(self):
            return self._pool.size

        def free(self):
            return self._pool.free()

        def running(self):
            return self._pool.running()

    pool = MyPool()
    srv = service.Service(
        Foobar, connection_factory=get_connection,
        exchange='testrpc', topic='test', pool=pool)

    srv.start()
    srv.consume_ready.wait()

    foobar = TestProxy(get_connection, timeout=3).test
    foobar.spam()

    srv.kill()
    assert pool.count == 1


def test_force_kill(get_connection):
    spam_continue = Event()
    spam_called = Event()

    class Foobar(object):
        def spam(self, context):
            spam_called.send(1)
            spam_continue.wait()

    srv = service.Service(
        Foobar, connection_factory=get_connection,
        exchange='testrpc', topic='test', poolsize=1)
    srv.start()
    eventlet.sleep()

    foobar = TestProxy(get_connection, timeout=3).test
    eventlet.spawn(foobar.spam)

    spam_called.wait()
    # spam will not complete, so we force it
    srv.kill(force=True)

    assert srv.greenlet.dead


def test_force_kill_no_workers(get_connection):
    spam_continue = Event()
    spam_called = Event()

    class Foobar(object):
        def spam(self, context):
            spam_called.send(1)
            spam_continue.wait()

    srv = service.Service(
        Foobar, connection_factory=get_connection,
        exchange='testrpc', topic='test', poolsize=1)
    srv.start()
    eventlet.sleep()

    foobar = TestProxy(get_connection, timeout=3).test
    eventlet.spawn(foobar.spam)

    spam_called.wait()
    spam_continue.send(2)
    # spam has completed - but force anyway
    srv.kill(force=True)

    assert srv.greenlet.dead


def test_service_dos(get_connection):
    spam_continue = Event()
    spam_called = Event()

    class Foobar(object):
        def spam(self, context, do_wait=True):
            if do_wait:
                spam_called.send(1)
                spam_continue.wait()

    s1 = service.Service(
        Foobar, connection_factory=get_connection,
        exchange='testrpc', topic='test', poolsize=1)
    s1.start()
    eventlet.sleep()

    foobar = TestProxy(get_connection, timeout=10).test
    # lets call spam such that it will block until we tell it to continue
    spam_call_1 = eventlet.spawn(foobar.spam, do_wait=True)
    eventlet.sleep()
    spam_called.wait()

    # At this point we will have exhausted it's prefetch count and worker-pool
    # s1 should not accept any more RPCs as it is busy processing spam().
    # Lets kick off two more RPC anyways, which should just wait in the queue
    spam_call_2 = eventlet.spawn(foobar.spam, do_wait=False)
    spam_call_3 = eventlet.spawn(foobar.spam, do_wait=False)
    eventlet.sleep()

    # lets fire up another service, which should take care of the 2 extra calls
    s2 = service.Service(
        Foobar, connection_factory=get_connection,
        exchange='testrpc', topic='test')

    s2.start()
    s2.consume_ready.wait()
    eventlet.sleep()

    try:
        with eventlet.timeout.Timeout(5):
            # as soon as s2 is up and running, the 2 extra calls should
            # get a reply
            spam_call_2.wait()
            spam_call_3.wait()

            # we can let the first call continue now
            spam_continue.send(1)
            spam_call_1.wait()
    except eventlet.timeout.Timeout as t:
        pytest.fail('waiting for 2nd server timed out: {}'.format(t))
    finally:
        s1.kill()
        s2.kill()
