from mock import Mock, patch
import pytest

from nameko.dependencies import (
    entrypoint, EntrypointProvider, get_entrypoint_providers,
    injection, InjectionProvider, get_injection_providers,
    DependencyFactory, DependencyTypeError, dependency,
    DependencyProvider, PROCESS_SHARED, CONTAINER_SHARED)
from nameko.containers import ServiceContainer, WorkerContext


class SharedProvider(DependencyProvider):
    pass


class NestedProvider(DependencyProvider):
    pass


@dependency
def shared_provider(*args, **kwargs):
    return DependencyFactory(SharedProvider, *args, **kwargs)


@dependency
def nested_provider(*args, **kwargs):
    return DependencyFactory(NestedProvider, *args, **kwargs)


class FooProvider(EntrypointProvider):

    shared_provider = shared_provider(shared=CONTAINER_SHARED)

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


@entrypoint
def foobar(*args, **kwargs):
    """foobar-doc"""
    return DependencyFactory(FooProvider, *args, **kwargs)


class BarProvider(InjectionProvider):

    nested_provider = nested_provider()
    shared_provider = shared_provider(shared=CONTAINER_SHARED)

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

    including_nested = list(get_entrypoint_providers(
        container, include_dependencies=True))
    assert len(including_nested) == 2

    assert set([type(dep) for dep in including_nested]) == set(
        [FooProvider, SharedProvider])


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

    including_nested = list(get_injection_providers(
        container, include_dependencies=True))
    assert len(including_nested) == 3

    assert set([type(dep) for dep in including_nested]) == set(
        [BarProvider, SharedProvider, NestedProvider])


def test_nested_dependencies(rabbit_config):

    container = Mock()
    container.config = rabbit_config

    bar_factory = DependencyFactory(BarProvider)
    bar = bar_factory.create_and_bind_instance("bar", container)

    dependencies = list(bar.nested_dependencies)
    assert len(dependencies) == 2
    assert dependencies[0].container == dependencies[1].container == container
    assert set([type(dep) for dep in dependencies]) == set([SharedProvider,
                                                           NestedProvider])


def test_dependency_instances_are_shared(container_factory, rabbit_config):

    example_providers = []

    class ExampleProvider(InjectionProvider):
        """ Example provider with two shared sub-dependencies - one at shared
        at the container level, one at the process level.
        """
        ct_shared = shared_provider(shared=CONTAINER_SHARED)
        process_shared = shared_provider(shared=PROCESS_SHARED)

        def __init__(self):
            # track ExampleProvider instances
            example_providers.append(self)

        def acquire_injection(self):
            pass

    @injection
    def injection_provider():
        return DependencyFactory(ExampleProvider)

    class Service():
        provider1 = injection_provider()
        provider2 = injection_provider()

    with patch('nameko.dependencies.shared_dependencies', {}) as shared_deps:
        container1 = container_factory(Service, rabbit_config)
        container2 = container_factory(Service, rabbit_config)

    # four shared providers should exist (two for each service)
    assert len(example_providers) == 4

    # but all four share their sub-dependencies
    # two containers in one process result in three sharing_keys
    assert set(shared_deps.keys()) == set([container1, container2,
                                           PROCESS_SHARED])

    # exactly three shared dependencies should have been instantiated
    all_dependencies = []
    for shared in shared_deps.values():
        all_dependencies.extend(shared.values())
    assert len(all_dependencies) == 3
    assert all(isinstance(dep, SharedProvider) for dep in all_dependencies)


def test_entrypoint_decorator_does_not_mutate_service():
    service = ExampleService()
    assert service.echo(1) == 1


def test_decorated_functions_must_return_dependency_factories():

    with pytest.raises(DependencyTypeError):
        @dependency
        def meth():
            pass
        meth()

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


def test_stringify():
    dep = DependencyProvider()
    assert str(dep).startswith('<DependencyProvider [unbound] at')

    container = Mock()
    container.service_name = 'foobar'
    dep.bind('spam', container)
    assert str(dep).startswith('<DependencyProvider [foobar.spam] at')
