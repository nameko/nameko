# coding: utf-8

from mock import Mock
import pytest

from nameko.extensions import (
    Extension, Entrypoint, DependencyProvider,
    is_dependency, is_entrypoint, is_extension)
from nameko.testing.services import entrypoint_hook
from nameko.testing.utils import get_extension


class SimpleExtension(Extension):
    pass


class SimpleDependencyProvider(DependencyProvider):
    ext = SimpleExtension()


class SimpleEntrypoint(Entrypoint):
    pass

simple = SimpleEntrypoint.decorator


class Service(object):
    name = "service"

    dep = SimpleDependencyProvider()

    @simple
    def meth1(self):
        pass

    @simple
    def meth2(self):
        pass


def test_entrypoint_uniqueness(container_factory):
    c1 = container_factory(Service, config={})
    c2 = container_factory(Service, config={})

    # entrypoint declarations are identical between containers
    c1_meth1_entrypoints = c1.service_cls.meth1.nameko_entrypoints
    c2_meth1_entrypoints = c2.service_cls.meth1.nameko_entrypoints
    assert c1_meth1_entrypoints == c2_meth1_entrypoints

    # entrypoint instances are different between containers
    c1_simple_meth1 = get_extension(c1, SimpleEntrypoint, method_name="meth1")
    c2_simple_meth1 = get_extension(c2, SimpleEntrypoint, method_name="meth1")
    assert c1_simple_meth1 != c2_simple_meth1

    # entrypoint instances are different within a container
    simple_meth1 = get_extension(c1, SimpleEntrypoint, method_name="meth1")
    simple_meth2 = get_extension(c1, SimpleEntrypoint, method_name="meth2")
    assert simple_meth1 != simple_meth2


def test_dependency_uniqueness(container_factory):
    c1 = container_factory(Service, config={})
    c2 = container_factory(Service, config={})

    # dependencyprovider declarations are identical between containers
    assert c1.service_cls.dep == c2.service_cls.dep

    # dependencyprovider instances are different between containers
    dep1 = get_extension(c1, SimpleDependencyProvider)
    dep2 = get_extension(c2, SimpleDependencyProvider)
    assert dep1 != dep2


def test_extension_uniqueness(container_factory):
    c1 = container_factory(Service, config={})
    c2 = container_factory(Service, config={})
    dep1 = get_extension(c1, SimpleDependencyProvider)
    dep2 = get_extension(c2, SimpleDependencyProvider)

    # extension declarations are identical between containers
    assert c1.service_cls.dep.ext == c2.service_cls.dep.ext

    # extension instances are different between dependencies
    assert dep1 != dep2
    assert dep1.ext != dep2.ext


def test_is_bound():
    container = Mock()

    ext = SimpleExtension()
    assert not ext.is_bound()
    bound = ext.bind(container)
    assert bound.is_bound()


def test_bound_extendions_cannot_be_bound():
    container = Mock()

    ext = SimpleExtension()
    bound = ext.bind(container)

    with pytest.raises(RuntimeError) as exc_info:
        bound.bind(container)
    assert str(exc_info.value) == "Cannot `bind` a bound extension."


def test_extension_defined_on_instance(container_factory):

    class ExtensionWithParams(Extension):
        def __init__(self, arg):
            self.arg = arg

    class DynamicDependencyProvider(DependencyProvider):
        def __init__(self, ext_arg):
            self.ext = ExtensionWithParams(ext_arg)

    class Service(object):
        name = "service"
        dep = DynamicDependencyProvider("argument_for_extension")

    container = container_factory(Service, {})
    container.start()

    assert len(container.extensions) == 2
    dyn_dep = get_extension(container, DynamicDependencyProvider)
    assert dyn_dep.ext.arg == "argument_for_extension"


def test_is_extension():
    ext = SimpleExtension()
    assert is_extension(ext)
    assert not is_extension(object)


def test_is_dependency():
    dep = SimpleDependencyProvider()
    assert is_dependency(dep)
    ext = SimpleExtension()
    assert not is_dependency(ext)


def test_is_entrypoint():
    entry = SimpleEntrypoint()
    assert is_entrypoint(entry)
    ext = SimpleExtension()
    assert not is_entrypoint(ext)


def test_entrypoint_decorator_does_not_mutate_service():

    class Service():
        name = "service"

        @simple
        def echo(self, arg):
            return arg

    service = Service()
    assert service.echo(1) == 1


@pytest.mark.parametrize("method_name, expected_args, expected_kwargs", [
    ("implicit_no_args", (), {}),
    ("explicit_no_args", (), {}),
    ("args", ("arg",), {}),
    ("kwargs", ("arg",), {"kwarg": "kwarg"}),
])
def test_entrypoint_decorator(method_name, expected_args, expected_kwargs,
                              container_factory):

    class ConfigurableEntrypoint(Entrypoint):
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    configurable = ConfigurableEntrypoint.decorator

    class Service():
        name = "service"

        @configurable
        def implicit_no_args(self):
            return True

        @configurable()
        def explicit_no_args(self):
            return True

        @configurable('arg')
        def args(self):
            return True

        @configurable('arg', kwarg="kwarg")
        def kwargs(self):
            return True

    container = container_factory(Service, {})
    container.start()

    with entrypoint_hook(container, method_name) as method:
        assert method()

    entrypoint = get_extension(container, Entrypoint, method_name=method_name)
    assert entrypoint.args == expected_args
    assert entrypoint.kwargs == expected_kwargs


def test_extension_str():
    container = Mock()

    ext = Extension()
    assert str(ext).startswith('<Extension [unbound] at')

    bound = ext.bind(container)
    assert str(bound).startswith("<Extension at")


def test_entrypoint_str():
    container = Mock()
    container.service_name = "sérvice"

    ext = Entrypoint()
    assert str(ext).startswith('<Entrypoint [unbound] at')

    bound = ext.bind(container, "føbar")
    assert str(bound).startswith("<Entrypoint [sérvice.føbar] at")


def test_dependency_str():
    container = Mock()
    container.service_name = "sérvice"

    ext = DependencyProvider()
    assert str(ext).startswith('<DependencyProvider [unbound] at')

    bound = ext.bind(container, "føbar")
    assert str(bound).startswith("<DependencyProvider [sérvice.føbar] at")
