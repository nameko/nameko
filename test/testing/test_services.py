from mock import Mock
import pytest

from nameko.extensions import DependencyProvider
from nameko.events import event_handler
from nameko.exceptions import ExtensionNotFound, MethodNotFound
from nameko.rpc import RpcProxy, rpc
from nameko.standalone.events import event_dispatcher
from nameko.standalone.rpc import ServiceRpcProxy
from nameko.testing.services import (
    entrypoint_hook, worker_factory, replace_dependencies, once,
    restrict_entrypoints, entrypoint_waiter, EntrypointWaiter)
from nameko.testing.utils import get_container


class LanguageReporter(DependencyProvider):
    """ Return the language given in the worker context data
    """
    def get_dependency(self, worker_ctx):
        def get_language():
            return worker_ctx.data['language']
        return get_language


handle_event = Mock()


@pytest.yield_fixture(autouse=True)
def reset_mock():
    yield
    handle_event.reset_mock()


class Service(object):
    name = "service"

    a = RpcProxy("service_a")
    language = LanguageReporter()

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
    b = RpcProxy("service_b")

    @rpc
    def remote_method(self, value):
        res = "{}-a".format(value)
        return self.b.remote_method(res)


class ServiceB(object):

    name = "service_b"
    c = RpcProxy("service_c")

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
        with entrypoint_waiter(service_container, 'handle'):
            handle(event_payload)
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

    with pytest.raises(ExtensionNotFound):
        with entrypoint_hook(container, method):
            pass


def test_entrypoint_hook_container_dying(container_factory, rabbit_config):
    class DependencyError(Exception):
        pass

    class BadDependency(DependencyProvider):
        def worker_setup(self, worker_ctx):
            raise DependencyError("Boom")

    class BadService(Service):
        bad = BadDependency()

    container = container_factory(BadService, rabbit_config)
    container.start()

    with pytest.raises(DependencyError):
        with entrypoint_hook(container, 'working') as call:
            call()


def test_worker_factory():

    class Service(object):
        name = "service"
        foo_proxy = RpcProxy("foo_service")
        bar_proxy = RpcProxy("bar_service")

    class OtherService(object):
        pass

    # simplest case, no overrides
    instance = worker_factory(Service)
    assert isinstance(instance, Service)
    assert isinstance(instance.foo_proxy, Mock)
    assert isinstance(instance.bar_proxy, Mock)

    # no dependencies to replace
    instance = worker_factory(OtherService)
    assert isinstance(instance, OtherService)

    # override specific dependency
    bar_dependency = object()
    instance = worker_factory(Service, bar_proxy=bar_dependency)
    assert isinstance(instance, Service)
    assert isinstance(instance.foo_proxy, Mock)
    assert instance.bar_proxy is bar_dependency

    # non-applicable dependency
    with pytest.raises(ExtensionNotFound):
        worker_factory(Service, nonexist=object())


def test_replace_dependencies(container_factory, rabbit_config):

    class Service(object):
        name = "service"
        foo_proxy = RpcProxy("foo_service")
        bar_proxy = RpcProxy("bar_service")
        baz_proxy = RpcProxy("baz_service")

        @rpc
        def method(self, arg):
            self.foo_proxy.remote_method(arg)

        @rpc
        def foo(self):
            return "bar"

    container = container_factory(Service, rabbit_config)

    # replace a single dependency
    foo_proxy = replace_dependencies(container, "foo_proxy")

    # replace multiple dependencies
    replacements = replace_dependencies(container, "bar_proxy", "baz_proxy")
    assert len([x for x in replacements]) == 2

    # verify that container.extensions doesn't include an RpcProxy anymore
    assert all([not isinstance(dependency, RpcProxy)
                for dependency in container.extensions])

    container.start()

    # verify that the mock dependency collects calls
    msg = "msg"
    with ServiceRpcProxy("service", rabbit_config) as service_proxy:
        service_proxy.method(msg)

    foo_proxy.remote_method.assert_called_once_with(msg)


def test_replace_non_dependency(container_factory, rabbit_config):

    class Service(object):
        name = "service"
        proxy = RpcProxy("foo_service")

        @rpc
        def method(self):
            pass

    container = container_factory(Service, rabbit_config)

    # error if dependency doesn't exit
    with pytest.raises(ExtensionNotFound):
        replace_dependencies(container, "nonexist")

    # error if dependency is not an dependency
    with pytest.raises(ExtensionNotFound):
        replace_dependencies(container, "method")


def test_replace_dependencies_container_already_started(container_factory,
                                                        rabbit_config):

    class Service(object):
        name = "service"
        proxy = RpcProxy("foo_service")

    container = container_factory(Service, rabbit_config)
    container.start()

    with pytest.raises(RuntimeError):
        replace_dependencies(container, "proxy")


def test_restrict_entrypoints(container_factory, rabbit_config):

    method_called = Mock()

    class Service(object):
        name = "service"

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
    with ServiceRpcProxy("service", rabbit_config) as service_proxy:
        with pytest.raises(MethodNotFound) as exc_info:
            service_proxy.handler_one("msg")
        assert str(exc_info.value) == "handler_one"

    # dispatch an event to handler_two
    msg = "msg"
    dispatch = event_dispatcher(rabbit_config)

    with entrypoint_waiter(container, 'handler_two'):
        dispatch('srcservice', 'eventtype', msg)

    # method_called should have exactly one call, derived from the event
    # handler and not from the disabled @once entrypoint
    method_called.assert_called_once_with(msg)


def test_restrict_nonexistent_entrypoint(container_factory, rabbit_config):

    class Service(object):
        name = "service"

        @rpc
        def method(self, arg):
            pass

    container = container_factory(Service, rabbit_config)

    with pytest.raises(ExtensionNotFound):
        restrict_entrypoints(container, "nonexist")


def test_restrict_entrypoint_container_already_started(container_factory,
                                                       rabbit_config):

    class Service(object):
        name = "service"

        @rpc
        def method(self, arg):
            pass

    container = container_factory(Service, rabbit_config)
    container.start()

    with pytest.raises(RuntimeError):
        restrict_entrypoints(container, "method")


def test_entrypoint_waiter(container_factory, rabbit_config):
    container = container_factory(Service, rabbit_config)
    container.start()

    dispatch = event_dispatcher(rabbit_config)
    with entrypoint_waiter(container, 'handle'):
        dispatch('srcservice', 'eventtype', "")


def test_entrypoint_waiter_timeout(container_factory, rabbit_config):
    container = container_factory(Service, rabbit_config)
    container.start()

    with pytest.raises(EntrypointWaiter.Timeout) as exc_info:
        with entrypoint_waiter(container, 'handle', timeout=0.01):
            pass
    assert str(exc_info.value) == (
        "Entrypoint service.handle failed to complete within 0.01 seconds")


def test_entrypoint_waiter_bad_entrypoint(container_factory, rabbit_config):
    container = container_factory(Service, rabbit_config)

    with pytest.raises(RuntimeError) as exc:
        with entrypoint_waiter(container, "unknown"):
            pass
    assert 'has no entrypoint' in str(exc)


def test_entrypoint_waiter_duplicates(container_factory, rabbit_config):
    container = container_factory(Service, rabbit_config)

    with pytest.raises(RuntimeError) as exc:
        with entrypoint_waiter(container, "working"):
            with entrypoint_waiter(container, "working"):
                pass
    assert 'already registered' in str(exc)
