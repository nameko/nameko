import eventlet
from mock import Mock
import pytest

from nameko.dependencies import InjectionProvider
from nameko.testing import patch_injection_provider
from nameko.testing.utils import (
    AnyInstanceOf, get_dependency, wait_for_call, instance_factory)


def test_patch_attr_dependency():

    class TestProvider(InjectionProvider):
        def acquire_injection(self, worker_ctx):
            return 'before patch injection'

    attr = TestProvider()
    patch_attr = patch_injection_provider(attr)

    # make sure patching does not happen until used as a contextmanager
    assert attr.acquire_injection(None) == 'before patch injection'

    with patch_attr as injected_mock:
        assert attr.acquire_injection(None) is injected_mock
        assert isinstance(injected_mock, Mock)


def test_any_instance_of():

    assert "" == AnyInstanceOf(str)
    assert 99 != AnyInstanceOf(str)

    class Foo(object):
        pass
    foo = Foo()

    assert foo == AnyInstanceOf(Foo)
    assert foo == AnyInstanceOf(object)
    assert foo != AnyInstanceOf(foo)

    assert repr(AnyInstanceOf(str)) == "<AnyInstanceOf-str>"
    assert repr(AnyInstanceOf(Foo)) == "<AnyInstanceOf-Foo>"

    assert AnyInstanceOf == AnyInstanceOf(type)
    assert str == AnyInstanceOf(type)
    assert int == AnyInstanceOf(type)
    assert type == AnyInstanceOf(type)


def test_wait_for_call():
    mock = Mock()

    def call_after(seconds):
        eventlet.sleep(seconds)
        mock.method()

    # should not raise
    eventlet.spawn(call_after, 0)
    with wait_for_call(1, mock.method):
        pass

    mock.reset_mock()

    with pytest.raises(eventlet.Timeout):
        eventlet.spawn(call_after, 1)
        with wait_for_call(0, mock.method):
            pass


def test_get_dependency(rabbit_config):

    from nameko.messaging import QueueConsumer
    from nameko.rpc import rpc, RpcProvider, RpcConsumer
    from nameko.containers import ServiceContainer, WorkerContext

    class Service(object):
        @rpc
        def foo(self):
            pass

        @rpc
        def bar(self):
            pass

    container = ServiceContainer(Service, WorkerContext, rabbit_config)

    rpc_consumer = get_dependency(container, RpcConsumer)
    queue_consumer = get_dependency(container, QueueConsumer)
    foo_rpc = get_dependency(container, RpcProvider, name="foo")
    bar_rpc = get_dependency(container, RpcProvider, name="bar")

    all_deps = container.dependencies
    assert all_deps == set([rpc_consumer, queue_consumer, foo_rpc, bar_rpc])


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
