from mock import Mock
import pytest

from nameko.events import event_handler
from nameko.rpc import rpc_proxy, rpc
from nameko.testing.service.integration import entrypoint_hook
from nameko.testing.utils import get_container, wait_for_call


class Service(object):

    a = rpc_proxy("service_a")

    @rpc
    def working(self, value):
        return self.a.remote_method(value)

    @rpc
    def broken(self, value):
        raise Error('broken')

    @event_handler('srcservice', 'eventtype')
    def handle(self, msg):
        handle_event(msg)


class ServiceA(object):

    name = "service_a"
    b = rpc_proxy("service_b")

    @rpc
    def remote_method(self, value):
        res = "{}-a".format(value)
        return self.b.remote_method(res)


class ServiceB(object):

    name = "service_b"
    c = rpc_proxy("service_c")

    @rpc
    def remote_method(self, value):
        res = "{}-b".format(value)
        return self.c.remote_method(res)


class ServiceC(object):

    name = "service_c"

    @rpc
    def remote_method(self, value):
        return "{}-c".format(value)


class Error(Exception):
    pass


handle_event = Mock()


@pytest.fixture(autouse=True)
def reset_mock():
    handle_event.reset_mock()


def test_entrypoint_hook(runner_factory, rabbit_config):

    service_classes = (Service, ServiceA, ServiceB, ServiceC)
    runner = runner_factory(rabbit_config, *service_classes)
    runner.start()

    service_container = get_container(runner, Service)

    event_payload = "msg"
    with entrypoint_hook(service_container, 'handle') as entrypoint:
        entrypoint(event_payload)

        with wait_for_call(1, handle_event):
            handle_event.assert_called_once_with(event_payload)


def test_entrypoint_hook_with_return(runner_factory, rabbit_config):

    service_classes = (Service, ServiceA, ServiceB, ServiceC)
    runner = runner_factory(rabbit_config, *service_classes)
    runner.start()

    service_container = get_container(runner, Service)

    with entrypoint_hook(service_container, 'working') as entrypoint:
        assert entrypoint("value") == "value-a-b-c"

    with entrypoint_hook(service_container, 'broken') as entrypoint:
        with pytest.raises(Error):
            entrypoint("value")
