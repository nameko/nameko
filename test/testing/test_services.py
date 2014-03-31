from mock import Mock
import pytest

from nameko.dependencies import (
    injection, entrypoint, InjectionProvider, EntrypointProvider,
    DependencyFactory)
from nameko.events import Event, event_handler
from nameko.exceptions import DependencyNotFound, MethodNotFound
from nameko.rpc import rpc_proxy, rpc
from nameko.standalone.events import event_dispatcher
from nameko.standalone.rpc import RpcProxy
from nameko.testing.services import (
    entrypoint_hook, worker_factory,
    replace_injections, restrict_entrypoints)
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


handle_event = Mock()


@pytest.fixture(autouse=True)
def reset_mock():
    handle_event.reset_mock()


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


def test_entrypoint_hook(runner_factory, rabbit_config):

    service_classes = (Service, ServiceA, ServiceB, ServiceC)
    runner = runner_factory(rabbit_config, *service_classes)
    runner.start()

    service_container = get_container(runner, Service)

    event_payload = "msg"
    with entrypoint_hook(service_container, 'handle') as handle:
        handle(event_payload)

        with wait_for_call(1, handle_event):
            handle_event.assert_called_once_with(event_payload)


def test_entrypoint_hook_with_return(runner_factory, rabbit_config):

    service_classes = (Service, ServiceA, ServiceB, ServiceC)
    runner = runner_factory(rabbit_config, *service_classes)
    runner.start()

    service_container = get_container(runner, Service)

    with entrypoint_hook(service_container, 'working') as working:
        assert working("value") == "value-a-b-c"

    with entrypoint_hook(service_container, 'broken') as broken:
        with pytest.raises(ExampleError):
            broken("value")


@pytest.mark.parametrize("context_data",
                         [{'language': 'en'}, {'language': 'fr'}])
def test_entrypoint_hook_context_data(container_factory, rabbit_config,
                                      context_data):

    container = container_factory(Service, rabbit_config)
    container.start()

    method = 'get_language'
    with entrypoint_hook(container, method, context_data) as get_language:
        assert get_language() == context_data['language']


def test_entrypoint_hook_dependency_not_found(container_factory,
                                              rabbit_config):

    container = container_factory(Service, rabbit_config)
    container.start()

    method = 'nonexistent_method'

    with pytest.raises(DependencyNotFound):
        with entrypoint_hook(container, method):
            pass


def test_worker_factory():

    class Service(object):
        foo_proxy = rpc_proxy("foo_service")
        bar_proxy = rpc_proxy("bar_service")

    class OtherService(object):
        pass

    # simplest case, no overrides
    instance = worker_factory(Service)
    assert isinstance(instance, Service)
    assert isinstance(instance.foo_proxy, Mock)
    assert isinstance(instance.bar_proxy, Mock)

    # no injections to replace
    instance = worker_factory(OtherService)
    assert isinstance(instance, OtherService)

    # override specific injection
    bar_injection = object()
    instance = worker_factory(Service, bar_proxy=bar_injection)
    assert isinstance(instance, Service)
    assert isinstance(instance.foo_proxy, Mock)
    assert instance.bar_proxy is bar_injection

    # non-applicable injection
    instance = worker_factory(Service, nonexist=object())
    assert isinstance(instance, Service)
    assert isinstance(instance.foo_proxy, Mock)
    assert isinstance(instance.bar_proxy, Mock)
    assert not hasattr(instance, "nonexist")


def test_replace_injections(container_factory, rabbit_config):

    class Service(object):
        foo_proxy = rpc_proxy("foo_service")
        bar_proxy = rpc_proxy("bar_service")
        baz_proxy = rpc_proxy("baz_service")

        @rpc
        def method(self, arg):
            self.foo_proxy.remote_method(arg)

        @rpc
        def foo(self):
            return "bar"

    container = container_factory(Service, rabbit_config)

    # replace a single injection
    foo_proxy = replace_injections(container, "foo_proxy")

    # replace multiple injections
    replacements = replace_injections(container, "bar_proxy", "baz_proxy")
    assert len([x for x in replacements]) == 2

    # verify that container.dependencies doesn't include an rpc_proxy anymore
    assert all([not isinstance(dependency, rpc_proxy.provider_cls)
                for dependency in container.dependencies])

    container.start()

    # verify that the mock injection collects calls
    msg = "msg"
    with RpcProxy("service", rabbit_config) as service_proxy:
        service_proxy.method(msg)

    foo_proxy.remote_method.assert_called_once_with(msg)


def test_replace_non_injection(container_factory, rabbit_config):

    class Service(object):
        proxy = rpc_proxy("foo_service")

        @rpc
        def method(self):
            pass

    container = container_factory(Service, rabbit_config)

    # error if dependency doesn't exit
    with pytest.raises(DependencyNotFound):
        replace_injections(container, "nonexist")

    # error if dependency is not an injection
    with pytest.raises(DependencyNotFound):
        replace_injections(container, "method")


def test_replace_injections_container_already_started(container_factory,
                                                      rabbit_config):

    class Service(object):
        proxy = rpc_proxy("foo_service")

    container = container_factory(Service, rabbit_config)
    container.start()

    with pytest.raises(RuntimeError):
        replace_injections(container, "proxy")


def test_restrict_entrypoints(container_factory, rabbit_config):

    method_called = Mock()

    class OnceProvider(EntrypointProvider):
        """ Entrypoint that spawns a worker exactly once, as soon as
        the service container started.
        """
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

        def start(self):
            self.container.spawn_worker(self, self.args, self.kwargs)

    @entrypoint
    def once(*args, **kwargs):
        return DependencyFactory(OnceProvider, args, kwargs)

    class ExampleEvent(Event):
        type = "eventtype"

    class Service(object):

        @rpc
        @once("assert not seen")
        def handler_one(self, arg):
            method_called(arg)

        @event_handler('srcservice', 'eventtype')
        def handler_two(self, msg):
            method_called(msg)

    container = container_factory(Service, rabbit_config)

    # disable the entrypoints on handler_one
    restrict_entrypoints(container, "handler_two")
    container.start()

    # verify the rpc entrypoint on handler_one is disabled
    with RpcProxy("service", rabbit_config) as service_proxy:
        with pytest.raises(MethodNotFound) as exc_info:
            service_proxy.handler_one("msg")
        assert exc_info.value.message == "handler_one"

    # dispatch an event to handler_two
    msg = "msg"
    with event_dispatcher('srcservice', rabbit_config) as dispatch:
        dispatch(ExampleEvent(msg))

    # method_called should have exactly one call, derived from the event
    # handler and not from the disabled @once entrypoint
    with wait_for_call(1, method_called):
        method_called.assert_called_once_with(msg)


def test_restrict_nonexistent_entrypoint(container_factory, rabbit_config):

    class Service(object):
        @rpc
        def method(self, arg):
            pass

    container = container_factory(Service, rabbit_config)

    with pytest.raises(DependencyNotFound):
        restrict_entrypoints(container, "nonexist")


def test_restrict_entrypoint_container_already_started(container_factory,
                                                       rabbit_config):

    class Service(object):
        @rpc
        def method(self, arg):
            pass

    container = container_factory(Service, rabbit_config)
    container.start()

    with pytest.raises(RuntimeError):
        restrict_entrypoints(container, "method")
