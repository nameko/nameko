import eventlet
import pytest

from nameko.events import event_handler, Event, BROADCAST
from nameko.standalone.events import event_dispatcher
from nameko.standalone.rpc import rpc_proxy
from nameko.rpc import rpc
from nameko.runners import ServiceRunner
from nameko.testing.utils import assert_stops_raising
from nameko.utils import Signal


class TestService1(object):
    name = 'foobar_1'


class TestService2(object):
    name = 'foobar_2'


received = []


@pytest.fixture(autouse=True)
def reset_mock():
    del received[:]


class TestEvent(Event):
    type = "testevent"


class Service(object):

    @rpc
    @event_handler("srcservice", TestEvent.type, handler_type=BROADCAST,
                   reliable_delivery=False)
    def handle(self, msg):
        received.append(msg)


def test_runner_lifecycle():
    events = []

    class Container(object):
        def __init__(self, service_cls, worker_ctx_cls, config):
            self.service_name = service_cls.__name__
            self.service_cls = service_cls
            self.worker_ctx_cls = worker_ctx_cls
            self.killed = Signal()

        def start(self):
            events.append(('start', self.service_cls.name, self.service_cls))

        def stop(self):
            events.append(('stop', self.service_cls.name, self.service_cls))

        def kill(self, exc):
            events.append(('kill', self.service_cls.name, self.service_cls))

        def wait(self):
            events.append(('wait', self.service_cls.name, self.service_cls))

    config = {}
    runner = ServiceRunner(config, container_cls=Container)

    runner.add_service(TestService1)
    runner.add_service(TestService2)

    runner.start()

    assert sorted(events) == [
        ('start', 'foobar_1', TestService1),
        ('start', 'foobar_2', TestService2),
    ]

    events = []
    runner.stop()
    assert sorted(events) == [
        ('stop', 'foobar_1', TestService1),
        ('stop', 'foobar_2', TestService2),
    ]

    events = []
    runner.kill(Exception('die'))
    assert sorted(events) == [
        ('kill', 'foobar_1', TestService1),
        ('kill', 'foobar_2', TestService2),
    ]

    events = []
    runner.wait()
    assert sorted(events) == [
        ('wait', 'foobar_1', TestService1),
        ('wait', 'foobar_2', TestService2),
    ]


def test_runner_waits_raises_error():
    class Container(object):
        def __init__(self, service_cls, worker_ctx_cls, config):
            self.killed = Signal()

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


def test_multiple_runners_coexist(runner_factory, rabbit_config,
                                  rabbit_manager):

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
    with event_dispatcher('srcservice', rabbit_config) as dispatch:
        dispatch(TestEvent(event_data))

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
    with rpc_proxy('service', rabbit_config) as proxy:
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
    with event_dispatcher('srcservice', rabbit_config) as dispatch:
        dispatch(TestEvent(event_data))

    with eventlet.Timeout(1):
        while len(received) == 0:
            eventlet.sleep()

        assert received == [event_data]

    # test rpc
    arg = "msg"
    del received[:]

    with rpc_proxy("service", rabbit_config) as proxy:
        proxy.handle(arg)

    with eventlet.Timeout(1):
        while len(received) == 0:
            eventlet.sleep()

        assert received == [arg]
