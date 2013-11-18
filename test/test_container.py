from mock import patch, call
from eventlet import spawn, sleep, Timeout
from eventlet.event import Event
import greenlet
import pytest

from nameko.containers import ServiceContainer, MAX_WOKERS_KEY, WorkerContext

from nameko.dependencies import(
    InjectionProvider, EntrypointProvider, entrypoint, injection,
    DependencyFactory)


class CallCollectorMixin(object):
    call_counter = 0

    def __init__(self):
        self.instances.add(self)
        self._reset_calls()

    def _reset_calls(self):
        self.calls = []
        self.call_ids = []

    def _log_call(self, data):
        CallCollectorMixin.call_counter += 1
        self.calls.append(data)
        self.call_ids.append(CallCollectorMixin.call_counter)

    def prepare(self):
        self._log_call(('prepare'))
        super(CallCollectorMixin, self).prepare()

    def start(self):
        self._log_call(('start'))
        super(CallCollectorMixin, self).start()

    def stop(self):
        self._log_call(('stop'))
        super(CallCollectorMixin, self).stop()

    def kill(self, exc):
        self._log_call(('kill'))
        super(CallCollectorMixin, self).stop()

    def worker_setup(self, worker_ctx):
        self._log_call(('setup', worker_ctx))
        super(CallCollectorMixin, self).worker_setup(worker_ctx)

    def worker_result(self, worker_ctx, result=None, exc=None):
        self._log_call(('result', worker_ctx, (result, exc)))
        super(CallCollectorMixin, self).worker_result(worker_ctx, result, exc)

    def worker_teardown(self, worker_ctx):
        self._log_call(('teardown', worker_ctx))
        super(CallCollectorMixin, self).worker_teardown(worker_ctx)


class CallCollectingEntrypointProvider(
        CallCollectorMixin, EntrypointProvider):
    instances = set()


class CallCollectingInjectionProvider(
        CallCollectorMixin, InjectionProvider):
    instances = set()

    def acquire_injection(self, worker_ctx):
        self._log_call(('acquire', worker_ctx))
        return 'spam-attr'


@entrypoint
def foobar():
    return DependencyFactory(CallCollectingEntrypointProvider)


@injection
def call_collector():
    return DependencyFactory(CallCollectingInjectionProvider)


egg_error = Exception('broken')


class Service(object):
    name = 'test-service'

    spam = call_collector()

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
    container = ServiceContainer(service_cls=Service,
                                 worker_ctx_cls=WorkerContext,
                                 config={})
    for dep in container.dependencies:
        dep._reset_calls()

    CallCollectorMixin.call_counter = 0
    return container


@pytest.yield_fixture
def logger():
    with patch('nameko.containers._log') as patched:
        yield patched


def test_collects_dependencies(container):
    assert len(container.dependencies) == 4
    assert container.dependencies == (
        CallCollectingEntrypointProvider.instances |
        CallCollectingInjectionProvider.instances)


def test_starts_dependencies(container):

    for dep in container.dependencies:
        assert dep.calls == []

    container.start()

    for dep in container.dependencies:
        assert dep.calls == [
            ('prepare'),
            ('start')
        ]


def test_stops_dependencies(container):

    container.stop()
    for dep in container.dependencies:
        assert dep.calls == [
            ('stop')
        ]


def test_stops_decdeps_before_attrdeps(container):
    container.stop()

    dependencies = container.dependencies
    spam_dep = next(iter(dependencies.injections))

    for dec_dep in dependencies.entrypoints:
        assert dec_dep.call_ids[0] < spam_dep.call_ids[0]


def test_worker_life_cycle(container):
    dependencies = container.dependencies

    (spam_dep,) = [dep for dep in dependencies if dep.name == 'spam']
    (ham_dep,) = [dep for dep in dependencies if dep.name == 'ham']
    (egg_dep,) = [dep for dep in dependencies if dep.name == 'egg']

    ham_worker_ctx = container.spawn_worker(ham_dep, [], {})
    container._worker_pool.waitall()
    egg_worker_ctx = container.spawn_worker(egg_dep, [], {})
    container._worker_pool.waitall()

    # TODO: test handle_result callback for spawn

    assert spam_dep.calls == [
        ('acquire', ham_worker_ctx),
        ('setup', ham_worker_ctx),
        ('result', ham_worker_ctx, ('ham', None)),
        ('teardown', ham_worker_ctx),
        ('acquire', egg_worker_ctx),
        ('setup', egg_worker_ctx),
        ('result', egg_worker_ctx, (None, egg_error)),
        ('teardown', egg_worker_ctx),
    ]

    assert ham_dep.calls == [
        ('setup', ham_worker_ctx),
        ('teardown', ham_worker_ctx),
        ('setup', egg_worker_ctx),
        ('teardown', egg_worker_ctx),
    ]

    assert egg_dep.calls == [
        ('setup', ham_worker_ctx),
        ('teardown', ham_worker_ctx),
        ('setup', egg_worker_ctx),
        ('teardown', egg_worker_ctx),
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

    container = ServiceContainer(service_cls=Service,
                                 worker_ctx_cls=WorkerContext,
                                 config={MAX_WOKERS_KEY: 1})

    dep = next(iter(container.dependencies))

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


def test_stop_already_stopped(container):

    assert not container._died.ready()
    container.stop()
    assert container._died.ready()

    with patch('nameko.containers._log') as logger:
        container.stop()
        assert logger.debug.call_args == call("already stopped %s", container)


def test_kill_already_stopped(container):

    assert not container._died.ready()
    container.stop()
    assert container._died.ready()

    with patch('nameko.containers._log') as logger:
        container.kill(Exception("kill"))
        assert logger.debug.call_args == call("already stopped %s", container)


def test_kill_misbehaving_dependency(container, logger):
    """ Break a dependency by making its ``kill`` method hang. The container
    should still exit.
    """
    dep = next(iter(container.dependencies))

    def sleep_forever(*args):
        while True:
            sleep()

    class Killed(Exception):
        pass
    exc = Killed("kill")

    with patch.object(dep, 'kill', sleep_forever):
        with patch('nameko.containers.KILL_TIMEOUT', 0):  # reduce kill timeout
            container.kill(exc)
            assert logger.warning.called
            assert logger.warning.call_args == call(
                "timeout waiting for dependencies.kill %s", container)

    with Timeout(1):
        with pytest.raises(Killed):
            container._died.wait()


def test_kill_container_with_active_workers(container):
    """ Start a worker that's not managed by dependencies. Ensure it is killed
    when the container is.
    """
    dep = next(iter(container.dependencies))
    container.spawn_worker(dep, ['sleep'], {})

    assert len(container._active_threads) == 1
    (worker_gt,) = container._active_threads

    class Killed(Exception):
        pass
    exc = Killed("kill")

    container.kill(exc)

    with Timeout(1):
        with pytest.raises(Killed):
            container._died.wait()

        with pytest.raises(greenlet.GreenletExit):
            worker_gt.wait()


def test_handle_killed_worker(container, logger):

    dep = next(iter(container.dependencies))
    container.spawn_worker(dep, ['sleep'], {})

    assert len(container._active_threads) == 1
    (worker_gt,) = container._active_threads

    worker_gt.kill()
    assert logger.warning.call_args == call("%s thread killed by container",
                                            container)

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
    exc = Exception('foobar')

    def raise_error():
        raise exc

    with patch.object(container, '_kill_dependencies',
                      side_effect=container._kill_dependencies) as kill_deps:
        container.start()
        # Any exception in a managed thread will cause the container
        # to be killed. Two threads raising an exception,
        # will cause the container's handle_thread_exit() method to
        # kill the container twice.
        container.spawn_managed_thread(raise_error)
        container.spawn_managed_thread(raise_error)

        with pytest.raises(Exception):
            container.wait()

        kill_deps.assert_called_once_with(exc)
