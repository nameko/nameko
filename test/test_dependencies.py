from mock import Mock
import pytest

from nameko.dependencies import (
    entrypoint, EntrypointProvider, get_entrypoint_providers,
    injection, InjectionProvider, get_injection_providers,
    DependencyFactory, DependencyTypeError, get_dependency_providers)
from nameko.messaging import QueueConsumer
from nameko.service import ServiceContainer, WorkerContext
from nameko.rpc import ReplyListener


class FooProvider(EntrypointProvider):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


@entrypoint
def foobar(*args, **kwargs):
    """foobar-doc"""
    return DependencyFactory(FooProvider, *args, **kwargs)


class BarProvider(InjectionProvider):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def acquire_injection(self):
        return lambda *args, **kwargs: "bar"


@injection
def barfoo(*args, **kwargs):
    return DependencyFactory(BarProvider, *args, **kwargs)


class ExampleService(object):

    injected = barfoo("arg", kwarg="kwarg")

    @foobar("arg", kwarg="kwarg")
    def echo(self, value):
        return value


def test_dependency_decorator():
    # make sure foobar is properly wrapped
    assert foobar.__doc__ == 'foobar-doc'
    assert foobar.func_name == 'foobar'

    def foo(spam):
        pass

    decorated_foo = foobar(foo='bar')(foo)

    # make sure dependency_deocorator passes through the decorated method
    assert decorated_foo is foo


def test_get_entrypoint_providers():

    config = Mock()
    container = ServiceContainer(ExampleService, WorkerContext, config)

    providers = list(get_entrypoint_providers(container))
    assert len(providers) == 1
    provider = providers[0]

    assert provider.name == "echo"
    assert isinstance(provider, FooProvider)
    assert provider.args == ("arg",)
    assert provider.kwargs == {"kwarg": "kwarg"}


def test_get_injection_providers():

    config = Mock()
    container = ServiceContainer(ExampleService, WorkerContext, config)

    providers = list(get_injection_providers(container))
    assert len(providers) == 1
    provider = providers[0]

    assert provider.name == "injected"
    assert isinstance(provider, BarProvider)
    assert provider.args == ("arg",)
    assert provider.kwargs == {"kwarg": "kwarg"}


def test_get_dependency_providers():

    from nameko.rpc import rpc_proxy

    class RpcService(object):
        rpc = rpc_proxy('foo')

    config = {'AMQP_URI': 'placeholder'}
    container = ServiceContainer(RpcService, WorkerContext, config)

    rpc_proxy = list(get_injection_providers(container))[0]
    dependencies = list(get_dependency_providers(container, rpc_proxy))

    assert len(dependencies) == 2
    assert dependencies[0].container == dependencies[1].container == container
    assert set([type(dep) for dep in dependencies]) == set([QueueConsumer,
                                                           ReplyListener])


def test_entrypoint_decorator_does_not_mutate_service():
    service = ExampleService()
    assert service.echo(1) == 1


def test_decorated_functions_must_return_dependency_factories():

    with pytest.raises(DependencyTypeError):
        @injection
        def foo():
            pass
        foo()

    with pytest.raises(DependencyTypeError):
        @entrypoint
        def bar():
            pass

        @bar
        def baz():
            pass
