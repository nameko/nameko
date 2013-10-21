from mock import patch, call
from eventlet import spawn, sleep, Timeout
from eventlet.event import Event
import pytest

from nameko.service import ServiceContainer, MAX_WOKERS_KEY, WorkerContext

from nameko.dependencies import(
    AttributeDependency, DecoratorDependency, dependency_decorator)


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

    def start(self, srv_ctx):
        self._log_call(('start', srv_ctx))
        super(CallCollectorMixin, self).start(srv_ctx)

    def on_container_started(self, srv_ctx):
        self._log_call(('started', srv_ctx))
        super(CallCollectorMixin, self).on_container_started(srv_ctx)

    def stop(self, srv_ctx):
        self._log_call(('stop', srv_ctx))
        super(CallCollectorMixin, self).stop(srv_ctx)

    def on_container_stopped(self, srv_ctx):
        self._log_call(('stopped', srv_ctx))
        super(CallCollectorMixin, self).on_container_stopped(srv_ctx)

    def call_setup(self, worker_ctx):
        self._log_call(('setup', worker_ctx))
        super(CallCollectorMixin, self).call_setup(worker_ctx)

    def call_teardown(self, worker_ctx):
        self._log_call(('teardown', worker_ctx))
        super(CallCollectorMixin, self).call_teardown(worker_ctx)


class CallCollectingDecoratorDependency(
        CallCollectorMixin, DecoratorDependency):
    instances = set()


class CallCollectingAttributeDependency(
        CallCollectorMixin, AttributeDependency):
    instances = set()

    def acquire_injection(self, worker_ctx):
        self._log_call(('acquire', worker_ctx))
        return 'spam-attr'

    def release_injection(self, worker_ctx):
        self._log_call(('release', worker_ctx))

    def call_result(self, worker_ctx, result=None, exc=None):
        self._log_call(('result', worker_ctx, (result, exc)))
        super(CallCollectorMixin, self).call_result(worker_ctx, result, exc)


@dependency_decorator
def foobar():
    dec = CallCollectingDecoratorDependency()
    return dec

egg_error = Exception('broken')


class Service(object):
    name = 'test-service'

    spam = CallCollectingAttributeDependency()

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
                                 config=None)
    for dep in container.dependencies:
        dep._reset_calls()

    CallCollectorMixin.call_counter = 0
    return container


def test_collects_dependencies(container):
    assert len(container.dependencies) == 4
    assert container.dependencies == (
        CallCollectingDecoratorDependency.instances |
        CallCollectingAttributeDependency.instances)


def test_starts_dependencies(container):
    srv_ctx = container.ctx

    for dep in container.dependencies:
        assert dep.calls == []

    container.start()

    for dep in container.dependencies:
        assert dep.calls == [
            ('start', srv_ctx),
            ('started', srv_ctx)
        ]


def test_stops_dependencies(container):
    srv_ctx = container.ctx

    container.stop()
    for dep in container.dependencies:
        assert dep.calls == [
            ('stop', srv_ctx),
            ('stopped', srv_ctx)
        ]


def test_stops_decdeps_before_attrdeps(container):
    container.stop()

    dependencies = container.dependencies
    spam_dep = next(iter(dependencies.attributes))

    for dec_dep in dependencies.decorators:
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
        ('setup', ham_worker_ctx),
        ('acquire', ham_worker_ctx),
        ('result', ham_worker_ctx, ('ham', None)),
        ('teardown', ham_worker_ctx),
        ('release', ham_worker_ctx),
        ('setup', egg_worker_ctx),
        ('acquire', egg_worker_ctx),
        ('result', egg_worker_ctx, (None, egg_error)),
        ('teardown', egg_worker_ctx),
        ('release', egg_worker_ctx),
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


def test_stop_waits_for_running_workers_before_signalling_container_stopped():
    spam_called = Event()
    container_stopped = Event()

    class StopDep(AttributeDependency):
        def acquire_injection(self, worker_ctx):
            return 'stop'

        def on_container_stopped(self, srv_ctx):
            # we should not see any running workers at this stage
            container_stopped.send(container._worker_pool.running())

    class Service(object):
        name = 'wait-for-worker'

        stop = StopDep()

        @foobar
        def spam(self, a):
            spam_called.send(a)
            sleep(0.01)

    container = ServiceContainer(service_cls=Service,
                                 worker_ctx_cls=WorkerContext,
                                 config=None)

    dep = next(iter(container.dependencies.decorators))
    container.spawn_worker(dep, ['ham'], {})

    with Timeout(1):
        spam_called.wait()
        spawn(container.stop)
        assert container_stopped.wait() == 0


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

    with patch('nameko.service._log') as logger:
        container.stop()
        assert logger.debug.call_args == call("already stopped %s", container)


def test_kill_already_stopped(container):

    assert not container._died.ready()
    container.stop()
    assert container._died.ready()

    with patch('nameko.service._log') as logger:
        container.kill(Exception("kill"))
        assert logger.debug.call_args == call("already stopped %s", container)


@pytest.yield_fixture
def logger():
    with patch('nameko.service._log') as patched:
        yield patched


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
        with patch('nameko.service.KILL_TIMEOUT', 0):  # reduce kill timeout
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

    assert len(container._active_workers) == 1
    worker_gt = container._active_workers[0]

    class Killed(Exception):
        pass
    exc = Killed("kill")

    container.kill(exc)

    with Timeout(1):
        with pytest.raises(Killed):
            container._died.wait()
        with pytest.raises(Killed):
            worker_gt.wait()


def test_handle_killed_worker(container, logger):

    dep = next(iter(container.dependencies))
    container.spawn_worker(dep, ['sleep'], {})

    assert len(container._active_workers) == 1
    worker_gt = container._active_workers[0]

    worker_gt.kill()
    assert logger.warning.call_args == call("%s worker killed",
                                            container.service_name,
                                            exc_info=True)

    assert not container._died.ready()  # container continues running
