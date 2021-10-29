import pytest
from mock import Mock, call

from nameko import config
from nameko.events import BROADCAST, event_handler
from nameko.rpc import rpc
from nameko.runners import ServiceRunner, run_services
from nameko.standalone.events import event_dispatcher
from nameko.standalone.rpc import ServiceRpcClient
from nameko.testing.services import entrypoint_waiter
from nameko.testing.utils import assert_stops_raising, get_container


class TestService1(object):
    name = 'foobar_1'


class TestService2(object):
    name = 'foobar_2'


@pytest.fixture
def tracker():
    return Mock()


@pytest.fixture
def service_cls(tracker):

    class Service(object):
        name = "service"

        @rpc
        @event_handler(
            "srcservice", "testevent",
            handler_type=BROADCAST, reliable_delivery=False
        )
        def handle(self, msg):
            tracker(msg)

    return Service


@config.patch({'SERVICE_CONTAINER_CLS': 'fake_module.ServiceContainer'})
def test_runner_lifecycle(fake_module):
    events = set()

    class Container(object):
        def __init__(self, service_cls):
            self.service_name = service_cls.__name__
            self.service_cls = service_cls

        def start(self):
            events.add(('start', self.service_cls.name, self.service_cls))

        def stop(self):
            events.add(('stop', self.service_cls.name, self.service_cls))

        def kill(self):
            events.add(('kill', self.service_cls.name, self.service_cls))

        def wait(self):
            events.add(('wait', self.service_cls.name, self.service_cls))

    fake_module.ServiceContainer = Container

    runner = ServiceRunner()

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


@config.patch({'SERVICE_CONTAINER_CLS': 'fake_module.ServiceContainer'})
def test_contextual_lifecycle(fake_module):
    events = set()

    class Container(object):
        def __init__(self, service_cls):
            self.service_name = service_cls.__name__
            self.service_cls = service_cls

        def start(self):
            events.add(('start', self.service_cls.name, self.service_cls))

        def stop(self):
            events.add(('stop', self.service_cls.name, self.service_cls))

        def kill(self, exc=None):
            events.add(('kill', self.service_cls.name, self.service_cls))

    fake_module.ServiceContainer = Container

    with run_services(TestService1, TestService2):
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
    with run_services(TestService1, TestService2, kill_on_exit=True):
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


@config.patch({'SERVICE_CONTAINER_CLS': 'fake_module.ServiceContainer'})
def test_runner_waits_raises_error(fake_module):
    class Container(object):
        def __init__(self, service_cls):
            pass

        def start(self):
            pass

        def stop(self):
            pass

        def wait(self):
            raise Exception('error in container')

    fake_module.ServiceContainer = Container

    runner = ServiceRunner()
    runner.add_service(TestService1)
    runner.start()

    with pytest.raises(Exception) as exc_info:
        runner.wait()
    assert exc_info.value.args == ('error in container',)


@pytest.mark.usefixtures("rabbit_config")
def test_multiple_runners_coexist(
    runner_factory, get_vhost, rabbit_manager, service_cls, tracker
):

    runner1 = runner_factory(service_cls)
    runner1.start()

    runner2 = runner_factory(service_cls)
    runner2.start()

    vhost = get_vhost(config['AMQP_URI'])

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
    event_data = "event"
    dispatch = event_dispatcher()

    container1 = list(runner1.containers)[0]
    container2 = list(runner2.containers)[0]

    with entrypoint_waiter(container1, "handle"):
        with entrypoint_waiter(container2, "handle"):
            dispatch('srcservice', "testevent", event_data)
    assert tracker.call_args_list == [call(event_data), call(event_data)]

    # verify there are two consumers on the rpc queue
    rpc_queue = rabbit_manager.get_queue(vhost, 'rpc-service')
    assert rpc_queue['consumers'] == 2

    # test rpc (only one service will respond)
    arg = "arg"
    with ServiceRpcClient('service') as client:
        client.handle(arg)

    assert tracker.call_args_list == [
        call(event_data), call(event_data), call(arg)
    ]


@pytest.mark.usefixtures("rabbit_config")
def test_runner_with_duplicate_services(
    runner_factory, service_cls, tracker
):

    # host Service multiple times
    runner = runner_factory()
    runner.add_service(service_cls)
    runner.add_service(service_cls)  # no-op
    runner.start()

    # it should only be hosted once
    assert len(runner.containers) == 1
    container = list(runner.containers)[0]

    # test events (only one service is hosted)
    event_data = "event"
    dispatch = event_dispatcher()

    with entrypoint_waiter(container, "handle"):
        dispatch('srcservice', "testevent", event_data)
    assert tracker.call_args_list == [call(event_data)]

    # test rpc
    arg = "arg"
    with ServiceRpcClient("service") as client:
        client.handle(arg)

    assert tracker.call_args_list == [call(event_data), call(arg)]


@pytest.mark.usefixtures("rabbit_config")
def test_runner_catches_managed_thread_errors(runner_factory, service_cls):

    class Broken(Exception):
        pass

    def raises():
        raise Broken('error')

    runner = runner_factory(service_cls)

    container = get_container(runner, service_cls)
    container.spawn_managed_thread(raises)

    with pytest.raises(Broken):
        runner.wait()
