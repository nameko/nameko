import sys

import eventlet
from eventlet import spawn, sleep, Timeout
from eventlet.event import Event
import greenlet
from mock import patch, call, ANY, Mock
import pytest

from nameko.containers import ServiceContainer
from nameko.constants import MAX_WORKERS_CONFIG_KEY
from nameko.extensions import DependencyProvider, Entrypoint
from nameko.testing.utils import get_extension


class CallCollectorMixin(object):
    call_counter = 0

    def __init__(self):
        self._reset_calls()

    def bind(self, *args):
        res = super(CallCollectorMixin, self).bind(*args)
        self.instances.add(res)
        return res

    def _reset_calls(self):
        self.calls = []
        self.call_ids = []

    def _log_call(self, data):
        CallCollectorMixin.call_counter += 1
        self.calls.append(data)
        self.call_ids.append(CallCollectorMixin.call_counter)

    def setup(self):
        self._log_call(('setup'))
        super(CallCollectorMixin, self).setup()

    def start(self):
        self._log_call(('start'))
        super(CallCollectorMixin, self).start()

    def stop(self):
        self._log_call(('stop'))
        super(CallCollectorMixin, self).stop()

    def kill(self, exc=None):
        self._log_call(('kill'))
        super(CallCollectorMixin, self).stop()


class CallCollectingEntrypoint(CallCollectorMixin, Entrypoint):
    instances = set()


class CallCollectingDependencyProvider(CallCollectorMixin, DependencyProvider):
    instances = set()

    def get_dependency(self, worker_ctx):
        self._log_call(('get_dependency', worker_ctx))
        return 'spam-attr'

    def worker_setup(self, worker_ctx):
        self._log_call(('worker_setup', worker_ctx))
        super(CallCollectorMixin, self).worker_setup(worker_ctx)

    def worker_result(self, worker_ctx, result=None, exc_info=None):
        self._log_call(('worker_result', worker_ctx, (result, exc_info)))
        super(CallCollectorMixin, self).worker_result(
            worker_ctx, result, exc_info)

    def worker_teardown(self, worker_ctx):
        self._log_call(('worker_teardown', worker_ctx))
        super(CallCollectorMixin, self).worker_teardown(worker_ctx)


foobar = CallCollectingEntrypoint.decorator

egg_error = Exception('broken')


class Service(object):
    name = 'test-service'

    spam = CallCollectingDependencyProvider()

    @foobar
    def ham(self):
        return 'ham'

    @foobar
    def egg(self):
        raise egg_error

    @foobar
    def wait(self):
        while True:
            sleep()


@pytest.fixture
def container():
    container = ServiceContainer(Service, config={})
    for ext in container.extensions:
        ext._reset_calls()

    CallCollectorMixin.call_counter = 0
    return container


@pytest.yield_fixture
def logger():
    with patch('nameko.containers._log', autospec=True) as patched:
        yield patched


def test_collects_extensions(container):
    assert len(container.extensions) == 4
    assert container.extensions == (
        CallCollectingEntrypoint.instances |
        CallCollectingDependencyProvider.instances)


def test_starts_extensions(container):

    for ext in container.extensions:
        assert ext.calls == []

    container.start()

    for ext in container.extensions:
        assert ext.calls == [
            ('setup'),
            ('start')
        ]


def test_stops_extensions(container):

    container.stop()
    for ext in container.extensions:
        assert ext.calls == [
            ('stop')
        ]


def test_stops_entrypoints_before_dependency_providers(container):
    container.stop()

    provider = get_extension(container, DependencyProvider)

    for entrypoint in container.entrypoints:
        assert entrypoint.call_ids[0] < provider.call_ids[0]


def test_worker_life_cycle(container):

    spam_dep = get_extension(container, DependencyProvider)
    ham_dep = get_extension(container, Entrypoint, method_name="ham")
    egg_dep = get_extension(container, Entrypoint, method_name="egg")

    handle_result = Mock()
    handle_result.side_effect = (
        lambda worker_ctx, res, exc_info: (res, exc_info))

    ham_worker_ctx = container.spawn_worker(
        ham_dep, [], {}, handle_result=handle_result)
    container._worker_pool.waitall()

    egg_worker_ctx = container.spawn_worker(
        egg_dep, [], {}, handle_result=handle_result)
    container._worker_pool.waitall()

    assert spam_dep.calls == [
        ('get_dependency', ham_worker_ctx),
        ('worker_setup', ham_worker_ctx),
        ('worker_result', ham_worker_ctx, ('ham', None)),
        ('worker_teardown', ham_worker_ctx),
        ('get_dependency', egg_worker_ctx),
        ('worker_setup', egg_worker_ctx),
        ('worker_result', egg_worker_ctx, (None, (Exception, egg_error, ANY))),
        ('worker_teardown', egg_worker_ctx),
    ]

    assert handle_result.call_args_list == [
        call(ham_worker_ctx, "ham", None),
        call(egg_worker_ctx, None, (Exception, egg_error, ANY))
    ]


def test_wait_waits_for_container_stopped(container):
    gt = spawn(container.wait)

    with Timeout(1):
        assert not gt.dead
        container.stop()
        sleep(0.01)
        assert gt.dead


def test_container_doesnt_exhaust_max_workers(container):
    spam_called = Event()
    spam_continue = Event()

    class Service(object):
        name = 'max-workers'

        @foobar
        def spam(self, a):
            spam_called.send(a)
            spam_continue.wait()

    container = ServiceContainer(Service, config={MAX_WORKERS_CONFIG_KEY: 1})

    dep = get_extension(container, Entrypoint)

    # start the first worker, which should wait for spam_continue
    container.spawn_worker(dep, ['ham'], {})

    # start the next worker in a speparate thread,
    # because it should block until the first one completed
    gt = spawn(container.spawn_worker, dep, ['eggs'], {})

    with Timeout(1):
        assert spam_called.wait() == 'ham'
        # if the container had spawned the second worker, we would see
        # an error indicating that spam_called was fired twice, and the
        # greenthread would now be dead.
        assert not gt.dead
        # reset the calls and allow the waiting worker to complete.
        spam_called.reset()
        spam_continue.send(None)
        # the second worker should now run and complete
        assert spam_called.wait() == 'eggs'
        assert gt.dead


def test_stop_already_stopped(container, logger):

    assert not container._died.ready()
    container.stop()
    assert container._died.ready()

    container.stop()
    assert logger.debug.call_args == call("already stopped %s", container)


def test_kill_already_stopped(container, logger):

    assert not container._died.ready()
    container.stop()
    assert container._died.ready()

    container.kill()
    assert logger.debug.call_args == call("already stopped %s", container)


def test_kill_container_with_active_threads(container):
    """ Start a thread that's not managed by dependencies. Ensure it is killed
    when the container is.
    """
    def sleep_forever():
        while True:
            sleep()

    container.spawn_managed_thread(sleep_forever)

    assert len(container._active_threads) == 1
    (worker_gt,) = container._active_threads

    container.kill()

    with Timeout(1):
        container._died.wait()

        with pytest.raises(greenlet.GreenletExit):
            worker_gt.wait()


def test_kill_container_with_protected_threads(container):
    """ Start a protected thread that's not managed by dependencies. Ensure it
    is killed when the container is.
    """
    def sleep_forever():
        while True:
            sleep()

    container.spawn_managed_thread(sleep_forever, protected=True)

    assert len(container._protected_threads) == 1
    (worker_gt,) = container._protected_threads

    container.kill()

    with Timeout(1):
        container._died.wait()

        with pytest.raises(greenlet.GreenletExit):
            worker_gt.wait()


def test_kill_container_with_active_workers(container_factory):
    waiting = Event()
    wait_forever = Event()

    class Service(object):
        name = 'kill-with-active-workers'

        @foobar
        def spam(self):
            waiting.send(None)
            wait_forever.wait()

    container = container_factory(Service, {})
    dep = get_extension(container, Entrypoint)

    # start the first worker, which should wait for spam_continue
    container.spawn_worker(dep, (), {})

    waiting.wait()

    with patch('nameko.containers._log') as logger:
        container.kill()
    calls = logger.warning.call_args_list
    assert call(
        'killing active thread for %s', dep
    ) in calls


def test_handle_killed_worker(container, logger):

    dep = get_extension(container, Entrypoint)
    container.spawn_worker(dep, ['sleep'], {})

    assert len(container._active_threads) == 1
    (worker_gt,) = container._active_threads

    worker_gt.kill()
    assert logger.debug.call_args == call(
        "%s thread killed by container", container)

    assert not container._died.ready()  # container continues running


def test_spawned_thread_kills_container(container):
    def raise_error():
        raise Exception('foobar')

    container.start()
    container.spawn_managed_thread(raise_error)

    with pytest.raises(Exception) as exc_info:
        container.wait()

    assert exc_info.value.args == ('foobar',)


def test_spawned_thread_causes_container_to_kill_other_thread(container):
    killed_by_error_raised = Event()

    def raise_error():
        raise Exception('foobar')

    def wait_forever():
        try:
            Event().wait()
        except:
            killed_by_error_raised.send()
            raise

    container.start()

    container.spawn_managed_thread(wait_forever)
    container.spawn_managed_thread(raise_error)

    with Timeout(1):
        killed_by_error_raised.wait()


def test_container_only_killed_once(container):

    class Broken(Exception):
        pass

    exc = Broken('foobar')

    def raise_error():
        raise exc

    with patch.object(
            container, '_kill_active_threads', autospec=True) as kill_threads:

        with patch.object(container, 'kill', wraps=container.kill) as kill:
            # insert an eventlet yield into the kill process, otherwise
            # the container dies before the second exception gets raised
            kill.side_effect = lambda exc: sleep()

            container.start()

            # any exception in a managed thread will cause the container
            # to be killed. Two threads raising an exception will cause
            # the container's handle_thread_exit() method to kill the
            # container twice.
            container.spawn_managed_thread(raise_error)
            container.spawn_managed_thread(raise_error)

            # container should die with an exception
            with pytest.raises(Broken):
                container.wait()

            # container.kill should have been called twice
            assert kill.call_args_list == [
                call((Broken, exc, ANY)),
                call((Broken, exc, ANY))
            ]

            # only the first kill results in any cleanup
            assert kill_threads.call_count == 1


def test_container_stop_kills_remaining_managed_threads(container, logger):
    """ Verify any remaining managed threads are killed when a container
    is stopped.
    """
    def sleep_forever():
        while True:
            sleep()

    container.start()

    container.spawn_managed_thread(sleep_forever)
    container.spawn_managed_thread(sleep_forever, protected=True)

    container.stop()

    assert logger.warning.call_args_list == [
        call("killing %s active thread(s)", 1),
        call("killing %s protected thread(s)", 1),
    ]

    assert logger.debug.call_args_list == [
        call("starting %s", container),
        call("stopping %s", container),
        call("%s thread killed by container", container),
        call("%s thread killed by container", container),
    ]


def test_container_kill_kills_remaining_managed_threads(container, logger):
    """ Verify any remaining managed threads are killed when a container
    is killed.
    """
    def sleep_forever():
        while True:
            sleep()

    container.start()

    container.spawn_managed_thread(sleep_forever)
    container.spawn_managed_thread(sleep_forever, protected=True)

    container.kill()

    assert logger.warning.call_args_list == [
        call("killing %s active thread(s)", 1),
        call("killing %s protected thread(s)", 1),
    ]

    assert logger.info.call_args_list == [
        call("killing %s", container),
    ]

    assert logger.debug.call_args_list == [
        call("starting %s", container),
        call("%s thread killed by container", container),
        call("%s thread killed by container", container),
    ]


def test_kill_bad_dependency(container):
    """ Verify that an exception from a badly-behaved dependency.kill()
    doesn't stop the container's kill process.
    """
    dep = get_extension(container, DependencyProvider)
    with patch.object(dep, 'kill') as dep_kill:
        dep_kill.side_effect = Exception('dependency error')

        container.start()

        # manufacture an exc_info to kill with
        try:
            raise Exception('container error')
        except:
            pass
        exc_info = sys.exc_info()

        container.kill(exc_info)

        with pytest.raises(Exception) as exc_info:
            container.wait()
        assert exc_info.value.message == "container error"


def test_stop_during_kill(container, logger):
    """ Verify we handle the race condition when a runner tries to stop
    a container while it is being killed.
    """
    with patch.object(
            container, '_kill_active_threads', autospec=True) as kill_threads:

        # force eventlet yield during kill() so stop() will be scheduled
        kill_threads.side_effect = eventlet.sleep

        # manufacture an exc_info to kill with
        try:
            raise Exception('error')
        except:
            pass
        exc_info = sys.exc_info()

        eventlet.spawn(container.kill, exc_info)
        eventlet.spawn(container.stop)

        with pytest.raises(Exception):
            container.wait()
        assert logger.debug.call_args_list == [
            call("already being killed %s", container),
        ]
