from mock import Mock
import pytest

from nameko.dependencies import Extension, Entrypoint, InjectionProvider
from nameko.testing.utils import get_dependency


class SimpleExtension(Extension):
    pass


class SimpleInjection(InjectionProvider):
    ext = SimpleExtension()


class SimpleEntrypoint(Entrypoint):
    pass

simple = SimpleEntrypoint.entrypoint


class Service(object):
    inj = SimpleInjection()

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
    c1_simple_meth1 = get_dependency(c1, SimpleEntrypoint, method_name="meth1")
    c2_simple_meth1 = get_dependency(c2, SimpleEntrypoint, method_name="meth1")
    assert c1_simple_meth1 != c2_simple_meth1

    # entrypoint instances are different within a container
    simple_meth1 = get_dependency(c1, SimpleEntrypoint, method_name="meth1")
    simple_meth2 = get_dependency(c1, SimpleEntrypoint, method_name="meth2")
    assert simple_meth1 != simple_meth2


def test_injection_uniqueness(container_factory):
    c1 = container_factory(Service, config={})
    c2 = container_factory(Service, config={})

    # injection declarations are identical between containers
    assert c1.service_cls.inj == c2.service_cls.inj

    # injection instances are different between containers
    inj1 = get_dependency(c1, SimpleInjection)
    inj2 = get_dependency(c2, SimpleInjection)
    assert inj1 != inj2


def test_extension_uniqueness(container_factory):
    c1 = container_factory(Service, config={})
    c2 = container_factory(Service, config={})
    inj1 = get_dependency(c1, SimpleInjection)
    inj2 = get_dependency(c2, SimpleInjection)

    # extension declarations are identical between containers
    assert c1.service_cls.inj.ext == c2.service_cls.inj.ext

    # extension instances are different between injections
    assert inj1 != inj2
    assert inj1.ext != inj2.ext


def test_clones_marked_as_clones():
    container = Mock()

    ext = SimpleExtension()
    assert ext._Extension__clone is False
    ext_clone = ext.clone(container)
    assert ext_clone._Extension__clone is True


def test_clones_cannot_be_cloned():
    container = Mock()

    ext = SimpleExtension()
    ext_clone = ext.clone(container)

    with pytest.raises(RuntimeError) as exc_info:
        ext_clone.clone(container)
    assert exc_info.value.message == "Cloned extensions cannot be cloned."


def test_extension_defined_on_instance(container_factory):

    class ExtensionWithParams(Extension):
        def __init__(self, arg):
            self.arg = arg

    class DynamicInjection(InjectionProvider):
        def __init__(self, ext_arg):
            self.ext = ExtensionWithParams(ext_arg)

    class Service(object):
        inj = DynamicInjection("argument_for_extension")

    container = container_factory(Service, {})
    container.start()

    assert len(container.dependencies) == 2
    dyn_inj = get_dependency(container, DynamicInjection)
    assert dyn_inj.ext.arg == "argument_for_extension"
