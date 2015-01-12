# coding: utf-8

import eventlet
from mock import Mock
import pytest

from nameko.dependencies import (
    Extension, Entrypoint, InjectionProvider, ProviderCollector)


class SharedProvider(Extension):
    pass


class NestedProvider(Extension):
    pass


class FooProvider(Entrypoint):
    shared_provider = SharedProvider(shared=True)

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


foobar = FooProvider.entrypoint


class BarProvider(InjectionProvider):

    nested_provider = NestedProvider()
    shared_provider = SharedProvider(shared=True)

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def acquire_injection(self):
        return lambda *args, **kwargs: "bar"


class ExampleService(object):

    injected = BarProvider("arg", kwarg="kwarg")

    @foobar("arg", kwarg="kwarg")
    def echo(self, value):
        return value


def test_entrypoint_decorator_does_not_mutate_service():
    service = ExampleService()
    assert service.echo(1) == 1


def test_str():
    dep = Extension()
    assert str(dep).startswith('<Extension [unbound] at')

    container = Mock()
    container.service_name = u'föbar'
    bound = dep.bind('spam', container)
    assert str(dep).startswith("<Extension [unbound] at")
    assert str(bound).startswith("<Extension [föbar.spam] at")


def test_provider_collector():
    collector = ProviderCollector()

    provider1 = object()
    provider2 = object()
    collector.register_provider(provider1)
    collector.register_provider(provider2)

    assert provider1 in collector._providers
    assert provider2 in collector._providers

    collector.unregister_provider(provider1)
    assert provider1 not in collector._providers

    # unregister missing provider is a no-op
    collector.unregister_provider(provider1)

    # stop() should block while a provider remains
    with pytest.raises(eventlet.Timeout):
        with eventlet.Timeout(0):
            collector.stop()

    # stop() will return when the final provider is unregistered
    with eventlet.Timeout(0):
        collector.unregister_provider(provider2)
        collector.stop()
