from eventlet.event import Event
from eventlet.greenpool import GreenPool
import eventlet
import pytest

from nameko import exceptions
from nameko import service

from nameko.testing import TestProxy


def test_service(get_connection):
    class Controller(object):
        def test_method(self, context, foo):
            return foo + 'spam'

    srv = service.Service(Controller, connection_factory=get_connection,
        exchange='testrpc', topic='test', )
    srv.start()
    eventlet.sleep()

    try:
        test = TestProxy(get_connection, timeout=3).test
        ret = test.test_method(foo='bar')

        assert ret == 'barspam'
    finally:
        srv.kill()


def test_service_doesnt_exhaust_pool(get_connection):
    POOLSIZE = 10

    class Controller(object):
        def test_method(self, context, foo):
            return foo + 'spam'

    srv = service.Service(Controller, connection_factory=get_connection,
        exchange='testrpc', topic='test', poolsize=POOLSIZE)
    srv.start()
    eventlet.sleep()

    try:
        with eventlet.Timeout(10):
            for i in range(POOLSIZE*2):
                test = TestProxy(get_connection, timeout=3).test
                ret = test.test_method(foo='bar')

    finally:
        srv.kill()



def test_exceptions(get_connection):
    class Controller(object):
        def test_method(self, context, **kwargs):
            raise KeyError('foo')

    srv = service.Service(Controller,
            connection_factory=get_connection,
            exchange='testrpc', topic='test', )
    srv.start()
    eventlet.sleep(0)

    try:
        test = TestProxy(get_connection, timeout=3).test
        with pytest.raises(exceptions.RemoteError):
            test.test_method()

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

    srv = service.Service(Foobar,
            connection_factory=get_connection,
            exchange='testrpc', topic='test', )

    srv.start()
    eventlet.sleep()

    kill_completed = Event()

    def kill():
        srv.kill()
        kill_completed.send(1)

    # we will wait until the service dies,
    # i.e. the queue consumers and any spawned worker
    waiter = eventlet.spawn(lambda: srv.wait())

    try:
        with eventlet.timeout.Timeout(5):
            foobar = TestProxy(get_connection, timeout=3).test
            # lets make sure we actually have a worker currently
            # processing an RPC call
            rpc_call = eventlet.spawn(foobar.spam)
            spam_called.wait()

            # we are in the middle of an RPC call, this will make sure
            # that wait() will have to wait for the worker and the queue
            eventlet.spawn(kill)
            # kill() does not actually kill anything, but rather just waits
            # for the consumer queue to stop.
            kill_completed.wait()
            # The consumer queue should be dead by now,
            # but we still have a worker running.
            assert waiter.dead == False
            # Let the worker finish it's job
            spam_continue.send(1)
            rpc_call.wait()
            # The call completed, there should be no more workers running
            # causing the service to die, which we have been waiting for.
            # TODO: should probably wait for the waiter at this point.
            assert waiter.dead

    except eventlet.timeout.Timeout as t:
        pytest.fail('waiting for death of service timed out: {}'.format(t))
    finally:
        srv.kill()


def test_service_wait_consumer_dies(get_connection):
    srv = service.Service(object,
            connection_factory=get_connection,
            exchange='testrpc', topic='test', )

    srv.start()
    eventlet.sleep()

    waiter = eventlet.spawn(lambda: srv.wait())
    eventlet.sleep()

    srv.greenlet.kill()
    # we don't expect any errors while waiting
    waiter.wait()


def test_service_link(get_connection):
    srv = service.Service(object,
            connection_factory=get_connection,
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
    srv = service.Service(object,
            connection_factory=get_connection,
            exchange='testrpc', topic='test')

    srv.start()
    with pytest.raises(RuntimeError):
        srv.start()

    srv.kill()


def test_service_custom_pool(get_connection):
    class Foobar(object):
        def spam(self, context, **kwargs):
            pass

    class MyPool(object):
        _pool = GreenPool()
        count = 0

        def spawn(self, *args, **kwargs):
            self.count += 1
            return self._pool.spawn(*args, **kwargs)

        def wait(self):
            return self._pool.wait()

        @property
        def size(self):
            return self._pool.size

    pool = MyPool()
    srv = service.Service(Foobar,
                    connection_factory=get_connection,
                    exchange='testrpc', topic='test',
                    pool=pool)

    srv.start()
    eventlet.sleep()

    foobar = TestProxy(get_connection, timeout=3).test
    foobar.spam()

    srv.kill()
    assert pool.count == 1


@pytest.mark.skipif('True')
def test_service_dos(get_connection):
    #TODO: this test hangs forever, though it should fail
    spam_continue = Event()
    spam_called = Event()

    class Foobar(object):
        def spam(self, context, do_wait=True):
            if do_wait:
                spam_called.send(1)
                spam_continue.wait()

    s1 = service.Service(Foobar,
                    connection=get_connection(),
                    exchange='testrpc', topic='test',
                    poolsize=1)
    s1.start()
    eventlet.sleep()

    foobar = TestProxy(get_connection, timeout=10).test
    # lets call spam such that it will block until we tell it to continue
    spam_call_1 = eventlet.spawn(foobar.spam, do_wait=True)
    eventlet.sleep()
    spam_called.wait()

    # At this point we will have exhausted the pool-size and
    # s1 should not accept any more RPCs as it is busy processing spam().
    # Lets kick off a 2nd RPC anyways, which should just wait in the queue
    spam_call_2 = eventlet.spawn(foobar.spam, do_wait=False)
    eventlet.sleep()

    # lets fire up another service, which should take care of the 2nd call
    s2 = service.Service(Foobar,
                    connection=get_connection(),
                    exchange='testrpc', topic='test')
    s2.start()
    s2.consume_ready.wait()
    eventlet.sleep()

    try:
        #TODO: somehow this hangns forever in case s1 blocks
        with eventlet.timeout.Timeout(5):
            #as soon as s2 is up and running, the 2nd call should get a reply
            spam_call_2.wait()

            # we can let the first call continue now
            spam_continue.send(1)
            spam_call_1.wait()
    except eventlet.timeout.Timeout as t:
        pytest.fail('waiting for 2nd server timed out: {}'.format(t))
    finally:
        s1.kill()
        s2.kill()



