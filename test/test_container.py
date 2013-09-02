from eventlet import spawn, sleep, Timeout
from eventlet.event import Event

import pytest

from nameko.service import ServiceContainer, MAX_WOKERS_KEY

from nameko.dependencies import(
    AttributeDependency, DecoratorDependency, dependency_decorator)


class CallCollectorMixin(object):
    call_time = 0

    def __init__(self):
        self.instances.add(self)
        self._reset_calls()

    def _reset_calls(self):
        self.calls = []
        self.call_times = []

    def _log_call(self, data):
        CallCollectorMixin.call_time += 1
        self.calls.append(data)
        self.call_times.append(CallCollectorMixin.call_time)

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


class DecDep(CallCollectorMixin, DecoratorDependency):
    instances = set()


class AttrDep(CallCollectorMixin, AttributeDependency):
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
    dec = DecDep()
    return dec

egg_error = Exception('broken')


class Service(object):
    spam = AttrDep()

    @foobar
    def ham(self):
        return 'ham'

    @foobar
    def egg(self):
        raise egg_error


@pytest.fixture
def container():
    container = ServiceContainer(service_cls=Service, config=None)
    for dep in container.dependencies:
        dep._reset_calls()

    CallCollectorMixin.call_time = 0
    return container


def test_collects_dependencies(container):
    assert len(container.dependencies) == 3
    assert container.dependencies == (DecDep.instances | AttrDep.instances)


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
        assert dec_dep.call_times[0] < spam_dep.call_times[0]


def test_woker_life_cycle(container):
    dependencies = container.dependencies

    (spam_dep,) = [dep for dep in dependencies if dep.name == 'spam']
    (ham_dep,) = [dep for dep in dependencies if dep.name == 'ham']
    (egg_dep,) = [dep for dep in dependencies if dep.name == 'egg']

    ham_worker_ctx = container.spawn_worker(ham_dep, [], {})
    container._worker_pool.waitall()
    egg_worker_ctx = container.spawn_worker(egg_dep, [], {})
    container._worker_pool.waitall()

    #TODO: test handle_result callback for spawn

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
        stop = StopDep()

        @foobar
        def spam(self, a):
            spam_called.send(a)
            sleep(0.01)

    container = ServiceContainer(service_cls=Service, config=None)

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


def test_container_doesnt_exhaust_max_workers():
    spam_called = Event()
    spam_continue = Event()

    class Service(object):
        @foobar
        def spam(self, a):
            spam_called.send(a)
            spam_continue.wait()

    container = ServiceContainer(service_cls=Service,
                                 config={MAX_WOKERS_KEY: 1})

    dep = next(iter(container.dependencies))

    container.spawn_worker(dep, ['ham'], {})
    gt = spawn(container.spawn_worker, dep, ['eggs'], {})

    with Timeout(1):
        assert spam_called.wait() == 'ham'
        # if the container had spawned multiple workers, we would have had
        # an error indicating spam_called being sent twice adn the 2nd spawn
        # whould already be dead
        assert not gt.dead
        spam_called.reset()
        spam_continue.send(None)
        assert spam_called.wait() == 'eggs'
        assert gt.dead


# def test_force_kill_no_workers(get_connection):
#     spam_continue = Event()
#     spam_called = Event()

#     class Foobar(object):
#         def spam(self, context):
#             spam_called.send(1)
#             spam_continue.wait()

#     srv = service.Service(
#         Foobar, connection_factory=get_connection,
#         exchange='testrpc', topic='test', poolsize=1)
#     srv.start()
#     eventlet.sleep()

#     foobar = TestProxy(get_connection, timeout=3).test
#     eventlet.spawn(foobar.spam)

#     spam_called.wait()
#     spam_continue.send(2)
#     # spam has completed - but force anyway
#     srv.kill(force=True)

#     assert srv.greenlet.dead
