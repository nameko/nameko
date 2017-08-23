from collections import defaultdict

import pytest
from mock import call

from nameko.extensions import DependencyProvider, Extension, SharedExtension
from nameko.testing.utils import get_extension


class CallCollectorMixin(object):

    calls = defaultdict(lambda: defaultdict(list))

    def start(self, *args, **kwargs):
        self.calls[type(self)]['start'].append(call(*args, **kwargs))
        super(CallCollectorMixin, self).start(*args, **kwargs)


@pytest.yield_fixture(autouse=True)
def reset():
    yield
    CallCollectorMixin.calls.clear()


def test_simple_sharing(container_factory):

    class SimpleSharedExtension(CallCollectorMixin, SharedExtension):
        pass

    class SimpleDependencyProvider(CallCollectorMixin, DependencyProvider):
        ext = SimpleSharedExtension()

    class Service(object):
        name = "service"
        dep_1 = SimpleDependencyProvider()
        dep_2 = SimpleDependencyProvider()

    container = container_factory(Service, {})
    container.start()

    assert len(container.extensions) == 3
    calls = CallCollectorMixin.calls
    assert len(calls[SimpleDependencyProvider]['start']) == 2
    assert len(calls[SimpleSharedExtension]['start']) == 1


def test_custom_sharing_key(container_factory):

    class CustomSharedExtension(CallCollectorMixin, SharedExtension):
        def __init__(self, arg):
            self.arg = arg

        @property
        def sharing_key(self):
            return (type(self), self.arg)

    class SimpleDependencyProvider(CallCollectorMixin, DependencyProvider):
        ext_a = CustomSharedExtension("a")
        ext_b = CustomSharedExtension("b")

    class Service(object):
        name = "service"
        dep_1 = SimpleDependencyProvider()
        dep_2 = SimpleDependencyProvider()

    container = container_factory(Service, {})
    container.start()

    assert len(container.extensions) == 4
    calls = CallCollectorMixin.calls
    assert len(calls[SimpleDependencyProvider]['start']) == 2
    assert len(calls[CustomSharedExtension]['start']) == 2

    dep_1 = get_extension(
        container, SimpleDependencyProvider, attr_name="dep_1")
    dep_2 = get_extension(
        container, SimpleDependencyProvider, attr_name="dep_2")

    assert dep_1.ext_a is not dep_2.ext_b
    assert dep_1.ext_a is dep_2.ext_a
    assert dep_1.ext_b is dep_2.ext_b


def test_shared_intermediate(container_factory):

    class DedicatedExtension(CallCollectorMixin, Extension):
        pass

    class SharedIntermediate(CallCollectorMixin, SharedExtension):
        ext = DedicatedExtension()

    class SimpleDependencyProvider(CallCollectorMixin, DependencyProvider):
        ext = SharedIntermediate()

    class Service(object):
        name = "service"
        dep_1 = SimpleDependencyProvider()
        dep_2 = SimpleDependencyProvider()

    container = container_factory(Service, {})
    container.start()

    assert len(container.extensions) == 4
    calls = CallCollectorMixin.calls
    assert len(calls[SimpleDependencyProvider]['start']) == 2
    assert len(calls[SharedIntermediate]['start']) == 1
    assert len(calls[DedicatedExtension]['start']) == 1


def test_shared_extension_uniqueness(container_factory):

    class SimpleSharedExtension(CallCollectorMixin, SharedExtension):
        pass

    class SimpleDependencyProvider(CallCollectorMixin, DependencyProvider):
        ext = SimpleSharedExtension()

    class Service(object):
        name = "service"
        dep_1 = SimpleDependencyProvider()
        dep_2 = SimpleDependencyProvider()

    c1 = container_factory(Service, {})
    c2 = container_factory(Service, {})

    # extension declarations are identical between containers
    assert c1.service_cls.dep_1.ext == c2.service_cls.dep_1.ext

    # extension instances are different between containers
    shared_1 = get_extension(c1, SimpleSharedExtension)
    shared_2 = get_extension(c2, SimpleSharedExtension)
    assert shared_1 is not shared_2
