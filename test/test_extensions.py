# coding: utf-8

from mock import Mock
import pytest

from nameko.extensions import (
    Extension, Entrypoint, Dependency,
    is_dependency, is_entrypoint, is_extension)
from nameko.testing.services import entrypoint_hook
from nameko.testing.utils import get_extension


class SimpleExtension(Extension):
    pass


class SimpleDependency(Dependency):
    ext = SimpleExtension()


class SimpleEntrypoint(Entrypoint):
    pass

simple = SimpleEntrypoint.decorator


class Service(object):
    inj = SimpleDependency()

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

    # injection declarations are identical between containers
    assert c1.service_cls.inj == c2.service_cls.inj

    # injection instances are different between containers
    inj1 = get_extension(c1, SimpleDependency)
    inj2 = get_extension(c2, SimpleDependency)
    assert inj1 != inj2


def test_extension_uniqueness(container_factory):
    c1 = container_factory(Service, config={})
    c2 = container_factory(Service, config={})
    inj1 = get_extension(c1, SimpleDependency)
    inj2 = get_extension(c2, SimpleDependency)

    # extension declarations are identical between containers
    assert c1.service_cls.inj.ext == c2.service_cls.inj.ext

    # extension instances are different between dependencies
    assert inj1 != inj2
    assert inj1.ext != inj2.ext


def test_clones_marked_as_clones():
    container = Mock()

    ext = SimpleExtension()
    assert ext._Extension__clone is False
    bound = ext.bind(container)
    assert bound._Extension__clone is True


def test_clones_cannot_be_cloned():
    container = Mock()

    ext = SimpleExtension()
    bound = ext.bind(container)

    with pytest.raises(RuntimeError) as exc_info:
        bound.bind(container)
    assert exc_info.value.message == "Cloned extensions cannot be cloned."


def test_extension_defined_on_instance(container_factory):

    class ExtensionWithParams(Extension):
        def __init__(self, arg):
            self.arg = arg

    class DynamicInjection(Dependency):
        def __init__(self, ext_arg):
            self.ext = ExtensionWithParams(ext_arg)

    class Service(object):
        inj = DynamicInjection("argument_for_extension")

    container = container_factory(Service, {})
    container.start()

    assert len(container.extensions) == 2
    dyn_inj = get_extension(container, DynamicInjection)
    assert dyn_inj.ext.arg == "argument_for_extension"


def test_is_extension():
    ext = SimpleExtension()
    assert is_extension(ext)


def test_is_dependency():
    dep = SimpleDependency()
    assert is_dependency(dep)


def test_is_entrypoint():
    entry = SimpleEntrypoint()
    assert is_entrypoint(entry)


def test_entrypoint_decorator_does_not_mutate_service():

    class Service():
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
    assert str(ext).startswith('<Extension [declaration] at')

    bound = ext.bind(container)
    assert str(bound).startswith("<Extension at")


def test_entrypoint_str():
    container = Mock()
    container.service_name = "sérvice"

    ext = Entrypoint()
    assert str(ext).startswith('<Entrypoint [declaration] at')

    bound = ext.bind(container, "føbar")
    assert str(bound).startswith("<Entrypoint [sérvice.føbar] at")


def test_dependency_str():
    container = Mock()
    container.service_name = "sérvice"

    ext = Dependency()
    assert str(ext).startswith('<Dependency [declaration] at')

    bound = ext.bind(container, "føbar")
    assert str(bound).startswith("<Dependency [sérvice.føbar] at")
