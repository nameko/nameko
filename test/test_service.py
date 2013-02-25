from eventlet.event import Event
from eventlet.greenpool import GreenPool
import eventlet
import pytest


from newrpc import context
from newrpc import exceptions
from newrpc import sending
from newrpc import service


class Proxy(object):
    def __init__(self, get_connection, timeout=1, service=None, method=None):
        self.get_connection = get_connection
        self.timeout = timeout
        self.service = service
        self.method = method

    def __getattr__(self, key):
        service = self.service

        if service is None:
            service = key
            method = None
        else:
            method = key

        return self.__class__(self.get_connection, self.timeout,
                                service, method)

    def __call__(self, **kwargs):
        ctx = context.get_admin_context()
        with self.get_connection() as conn:
            return sending.send_rpc(conn, ctx,
                'testrpc', self.service, self.method, args=kwargs,
                timeout=self.timeout)


def test_service(connection, get_connection):
    class Controller(object):
        def test_method(self, context, foo):
            return foo + 'spam'

    srv = service.Service(Controller, connection=connection,
        exchange='testrpc', topic='test', )
    srv.start()
    eventlet.sleep()

    try:
        test = Proxy(get_connection, timeout=3).test
        ret = test.test_method(foo='bar')

        assert ret == 'barspam'
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
        test = Proxy(get_connection, timeout=3).test
        with pytest.raises(exceptions.RemoteError):
            test.test_method()

    finally:
        srv.kill()
        connection.release()


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
            connection=get_connection(),
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
            foobar = Proxy(get_connection, timeout=3).test
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


def test_service_wait_consumer_dies(get_connection):
    srv = service.Service(object,
            connection=get_connection(),
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
            connection=get_connection(),
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


def test_service_cannot_be_starte_twice(get_connection):
    srv = service.Service(object,
            connection=get_connection(),
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

    pool = MyPool()
    srv = service.Service(Foobar,
                    connection=get_connection(),
                    exchange='testrpc', topic='test',
                    pool=pool)

    srv.start()
    eventlet.sleep()

    foobar = Proxy(get_connection, timeout=3).test
    foobar.spam()

    assert pool.count == 1
