from mock import Mock
import pytest

from nameko.dependencies import injection, InjectionProvider, DependencyFactory
from nameko.events import event_handler
from nameko.rpc import rpc_proxy, rpc
from nameko.testing.services import entrypoint_hook, instance_factory
from nameko.testing.utils import get_container, wait_for_call


class LanguageReporter(InjectionProvider):
    """ Return the language given in the worker context data
    """
    def acquire_injection(self, worker_ctx):
        def get_language():
            return worker_ctx.data['language']
        return get_language


@injection
def language_reporter():
    return DependencyFactory(LanguageReporter)


class Service(object):

    a = rpc_proxy("service_a")
    language = language_reporter()

    @rpc
    def working(self, value):
        return self.a.remote_method(value)

    @rpc
    def broken(self, value):
        raise ExampleError('broken')

    @event_handler('srcservice', 'eventtype')
    def handle(self, msg):
        handle_event(msg)

    @rpc
    def get_language(self):
        return self.language()


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


class ExampleError(Exception):
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
        with pytest.raises(ExampleError):
            entrypoint("value")


@pytest.mark.parametrize("context_data",
                         [{'language': 'en'}, {'language': 'fr'}])
def test_entrypoint_hook_context_data(container_factory, rabbit_config,
                                      context_data):

    container = container_factory(Service, rabbit_config)
    container.start()

    method = 'get_language'
    with entrypoint_hook(container, method, context_data) as get_language:
        assert get_language() == context_data['language']


def test_instance_factory():

    from nameko.rpc import rpc_proxy

    class Service(object):
        foo_proxy = rpc_proxy("foo_service")
        bar_proxy = rpc_proxy("bar_service")

    class OtherService(object):
        pass

    # simplest case, no overrides
    instance = instance_factory(Service)
    assert isinstance(instance, Service)
    assert isinstance(instance.foo_proxy, Mock)
    assert isinstance(instance.bar_proxy, Mock)

    # no injections to replace
    instance = instance_factory(OtherService)
    assert isinstance(instance, OtherService)

    # override specific injection
    bar_injection = object()
    instance = instance_factory(Service, bar_proxy=bar_injection)
    assert isinstance(instance, Service)
    assert isinstance(instance.foo_proxy, Mock)
    assert instance.bar_proxy is bar_injection

    # non-applicable injection
    instance = instance_factory(Service, nonexist=object())
    assert isinstance(instance, Service)
    assert isinstance(instance.foo_proxy, Mock)
    assert isinstance(instance.bar_proxy, Mock)
    assert not hasattr(instance, "nonexist")
