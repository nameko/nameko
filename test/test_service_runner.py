import eventlet
import pytest
from mock import ANY, call, patch

from nameko.containers import ServiceContainer, WorkerContext
from nameko.events import BROADCAST, event_handler
from nameko.rpc import rpc
from nameko.runners import ServiceRunner, run_services
from nameko.standalone.events import event_dispatcher
from nameko.standalone.rpc import ServiceRpcProxy
from nameko.testing.services import dummy
from nameko.testing.utils import assert_stops_raising, get_container


class TestService1(object):
    name = 'foobar_1'


class TestService2(object):
    name = 'foobar_2'


received = []


@pytest.yield_fixture(autouse=True)
def reset_mock():
    yield
    del received[:]


@pytest.yield_fixture
def warnings():
    with patch('nameko.runners.warnings') as patched:
        yield patched


class Service(object):
    name = "service"

    @rpc
    @event_handler(
        "srcservice", "testevent",
        handler_type=BROADCAST, reliable_delivery=False
    )
    def handle(self, msg):
        received.append(msg)


def test_runner_lifecycle():
    events = set()

    class Container(object):
        def __init__(self, service_cls, worker_ctx_cls, config):
            self.service_name = service_cls.__name__
            self.service_cls = service_cls
            self.worker_ctx_cls = worker_ctx_cls

        def start(self):
            events.add(('start', self.service_cls.name, self.service_cls))

        def stop(self):
            events.add(('stop', self.service_cls.name, self.service_cls))

        def kill(self):
            events.add(('kill', self.service_cls.name, self.service_cls))

        def wait(self):
            events.add(('wait', self.service_cls.name, self.service_cls))

    config = {}
    runner = ServiceRunner(config, container_cls=Container)

    runner.add_service(TestService1)
    runner.add_service(TestService2)

    runner.start()

    assert events == {
        ('start', 'foobar_1', TestService1),
        ('start', 'foobar_2', TestService2),
    }

    events = set()
    runner.stop()
    assert events == {
        ('stop', 'foobar_1', TestService1),
        ('stop', 'foobar_2', TestService2),
    }

    events = set()
    runner.kill()
    assert events == {
        ('kill', 'foobar_1', TestService1),
        ('kill', 'foobar_2', TestService2),
    }

    events = set()
    runner.wait()
    assert events == {
        ('wait', 'foobar_1', TestService1),
        ('wait', 'foobar_2', TestService2),
    }


class TestRunnerCustomServiceContainerCls(object):

    @pytest.fixture
    def service_cls(self):

        class Service(object):
            name = "service"

            @dummy
            def method(self):
                pass

        return Service

    @pytest.fixture
    def container_cls(self, fake_module):

        class ServiceContainerX(ServiceContainer):
            pass

        fake_module.ServiceContainerX = ServiceContainerX
        return ServiceContainerX

    def test_config_key(self, service_cls, container_cls):
        config = {
            'SERVICE_CONTAINER_CLS': "fake_module.ServiceContainerX"
        }
        runner = ServiceRunner(config)
        runner.add_service(service_cls)

        container = get_container(runner, service_cls)
        assert isinstance(container, container_cls)

    def test_kwarg_deprecation_warning(
        self, warnings, service_cls, container_cls
    ):
        config = {}
        runner = ServiceRunner(config, container_cls=container_cls)
        runner.add_service(service_cls)

        container = get_container(runner, service_cls)
        assert isinstance(container, container_cls)

        # TODO: replace with pytest.warns when eventlet >= 0.19.0 is released
        assert warnings.warn.call_args_list == [call(ANY, DeprecationWarning)]


class TestRunnerCustomWorkerCtxCls(object):

    @pytest.fixture
    def service_cls(self):

        class Service(object):
            name = "service"

            @dummy
            def method(self):
                pass

        return Service

    @pytest.fixture
    def worker_ctx_cls(self, fake_module):

        class WorkerContextX(WorkerContext):
            pass

        fake_module.WorkerContextX = WorkerContextX
        return WorkerContextX

    def test_kwarg_deprecation_warning(
        self, warnings, service_cls, worker_ctx_cls
    ):
        config = {}
        runner = ServiceRunner(config)
        runner.add_service(service_cls, worker_ctx_cls=worker_ctx_cls)

        container = get_container(runner, service_cls)
        entrypoint = list(container.entrypoints)[0]

        worker_ctx = container.spawn_worker(entrypoint, (), {})
        assert isinstance(worker_ctx, worker_ctx_cls)

        # TODO: replace with pytest.warns when eventlet >= 0.19.0 is released
        assert warnings.warn.call_args_list == [call(ANY, DeprecationWarning)]


def test_contextual_lifecycle():
    events = set()

    class Container(object):
        def __init__(self, service_cls, worker_ctx_cls, config):
            self.service_name = service_cls.__name__
            self.service_cls = service_cls
            self.worker_ctx_cls = worker_ctx_cls

        def start(self):
            events.add(('start', self.service_cls.name, self.service_cls))

        def stop(self):
            events.add(('stop', self.service_cls.name, self.service_cls))

        def kill(self, exc=None):
            events.add(('kill', self.service_cls.name, self.service_cls))

        def wait(self):
            events.add(('wait', self.service_cls.name, self.service_cls))

    config = {}

    with run_services(config, TestService1, TestService2,
                      container_cls=Container):
        # Ensure the services were started
        assert events == {
            ('start', 'foobar_1', TestService1),
            ('start', 'foobar_2', TestService2),
        }

    # ...and that they were stopped
    assert events == {
        ('start', 'foobar_1', TestService1),
        ('start', 'foobar_2', TestService2),
        ('stop', 'foobar_1', TestService1),
        ('stop', 'foobar_2', TestService2),
    }

    events = set()
    with run_services(config, TestService1, TestService2,
                      container_cls=Container, kill_on_exit=True):
        # Ensure the services were started
        assert events == {
            ('start', 'foobar_1', TestService1),
            ('start', 'foobar_2', TestService2),
        }

    # ...and that they were killed
    assert events == {
        ('kill', 'foobar_1', TestService1),
        ('kill', 'foobar_2', TestService2),
        ('start', 'foobar_1', TestService1),
        ('start', 'foobar_2', TestService2),
    }


class TestContextualRunnerDeprecationWarnings(object):

    def test_container_cls_warning(self, warnings):

        class ServiceContainerX(ServiceContainer):
            pass

        config = {}
        with run_services(config, container_cls=ServiceContainerX):
            pass

        # TODO: replace with pytest.warns when eventlet >= 0.19.0 is released
        assert warnings.warn.call_args_list == [
            # from contextual runner
            call(ANY, DeprecationWarning),
            # from underlying ServiceRunner constructor
            call(ANY, DeprecationWarning),
        ]

    def test_worker_ctx_cls_warning(self, warnings):

        class WorkerContextX(WorkerContext):
            pass

        config = {}
        with run_services(config, worker_ctx_cls=WorkerContext):
            pass

        # TODO: replace with pytest.warns when eventlet >= 0.19.0 is released
        assert warnings.warn.call_args_list == [
            # from contextual runner
            # (no calls to ServiceRunner.add_service in this test)
            call(ANY, DeprecationWarning),
        ]


def test_runner_waits_raises_error():
    class Container(object):
        def __init__(self, service_cls, worker_ctx_cls, config):
            pass

        def start(self):
            pass

        def stop(self):
            pass

        def kill(self, exc):
            pass

        def wait(self):
            raise Exception('error in container')

    runner = ServiceRunner(config={}, container_cls=Container)
    runner.add_service(TestService1)
    runner.start()

    with pytest.raises(Exception) as exc_info:
        runner.wait()
    assert exc_info.value.args == ('error in container',)


def test_multiple_runners_coexist(
    runner_factory, rabbit_config, rabbit_manager
):

    runner1 = runner_factory(rabbit_config, Service)
    runner1.start()

    runner2 = runner_factory(rabbit_config, Service)
    runner2.start()

    vhost = rabbit_config['vhost']

    # verify there are two event queues with a single consumer each
    def check_consumers():
        evt_queues = [queue for queue in rabbit_manager.get_queues(vhost)
                      if queue['name'].startswith('evt-srcservice-testevent')]
        assert len(evt_queues) == 2
        for queue in evt_queues:
            assert queue['consumers'] == 1

    # rabbit's management API seems to lag
    assert_stops_raising(check_consumers)

    # test events (both services will receive if in "broadcast" mode)
    event_data = "msg"
    dispatch = event_dispatcher(rabbit_config)
    dispatch('srcservice', "testevent", event_data)

    with eventlet.Timeout(1):
        while len(received) < 2:
            eventlet.sleep()

        assert received == [event_data, event_data]

    # verify there are two consumers on the rpc queue
    rpc_queue = rabbit_manager.get_queue(vhost, 'rpc-service')
    assert rpc_queue['consumers'] == 2

    # test rpc (only one service will respond)
    del received[:]
    arg = "msg"
    with ServiceRpcProxy('service', rabbit_config) as proxy:
        proxy.handle(arg)

    with eventlet.Timeout(1):
        while len(received) == 0:
            eventlet.sleep()

        assert received == [arg]


def test_runner_with_duplicate_services(runner_factory, rabbit_config):

    # host Service multiple times
    runner = runner_factory(rabbit_config)
    runner.add_service(Service)
    runner.add_service(Service)  # no-op
    runner.start()

    # it should only be hosted once
    assert len(runner.containers) == 1

    # test events (only one service is hosted)
    event_data = "msg"
    dispatch = event_dispatcher(rabbit_config)
    dispatch('srcservice', 'testevent', event_data)

    with eventlet.Timeout(1):
        while len(received) == 0:
            eventlet.sleep()

        assert received == [event_data]

    # test rpc
    arg = "msg"
    del received[:]

    with ServiceRpcProxy("service", rabbit_config) as proxy:
        proxy.handle(arg)

    with eventlet.Timeout(1):
        while len(received) == 0:
            eventlet.sleep()

        assert received == [arg]


def test_runner_catches_managed_thread_errors(runner_factory, rabbit_config):

    class Broken(Exception):
        pass

    def raises():
        raise Broken('error')

    runner = runner_factory(rabbit_config, Service)

    container = get_container(runner, Service)
    container.spawn_managed_thread(raises)

    with pytest.raises(Broken):
        runner.wait()
