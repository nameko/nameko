from collections import defaultdict

from mock import call

from nameko.dependencies import InjectionProvider, Entrypoint, Extension
from nameko.testing.utils import get_dependency


class CallCollectorMixin(object):

    calls = defaultdict(lambda: defaultdict(list))

    def start(self, *args, **kwargs):
        self.calls[type(self)]['start'].append(call(*args, **kwargs))
        super(CallCollectorMixin, self).start(*args, **kwargs)


def test_injections_cannot_be_shared():

    class SimpleInjection(InjectionProvider):
        pass

    class Service(object):
        inj = SimpleInjection(shared=True)

    assert Service.inj._Extension__shared is False


def test_entrypoints_cannot_be_shared():

    class SimpleEntrypoint(Entrypoint):
        pass

    simple = SimpleEntrypoint.entrypoint

    class Service(object):

        @simple(shared=True)
        def method(self):
            pass

    entrypoint = list(Service.method.nameko_entrypoints)[0]
    assert entrypoint._Extension__shared is False


def test_simple_sharing(container_factory):

    class SharedExtension(CallCollectorMixin, Extension):
        pass

    class SimpleInjection(CallCollectorMixin, InjectionProvider):
        ext = SharedExtension(shared=True)

    class Service(object):
        inj_1 = SimpleInjection()
        inj_2 = SimpleInjection()

    container = container_factory(Service, {})
    container.start()

    assert len(container.dependencies) == 3
    assert len(CallCollectorMixin.calls[SimpleInjection]['start']) == 2
    assert len(CallCollectorMixin.calls[SharedExtension]['start']) == 1


def test_custom_sharing_key(container_factory):

    class SharedExtension(CallCollectorMixin, Extension):
        def __init__(self, arg, **kwargs):
            self.arg = arg
            super(SharedExtension, self).__init__(arg, **kwargs)

        @property
        def sharing_key(self):
            return (type(self), self.arg)

    class SimpleInjection(CallCollectorMixin, InjectionProvider):
        ext_a = SharedExtension("a", shared=True)
        ext_b = SharedExtension("b", shared=True)

    class Service(object):
        inj_1 = SimpleInjection()
        inj_2 = SimpleInjection()

    container = container_factory(Service, {})
    container.start()

    assert len(container.dependencies) == 4
    assert len(CallCollectorMixin.calls[SimpleInjection]['start']) == 2
    assert len(CallCollectorMixin.calls[SharedExtension]['start']) == 2

    inj_1 = get_dependency(container, SimpleInjection, name="inj_1")
    inj_2 = get_dependency(container, SimpleInjection, name="inj_2")

    assert inj_1.ext_a != inj_1.ext_b
    assert inj_1.ext_a is inj_2.ext_a
    assert inj_1.ext_b is inj_2.ext_b


def test_shared_intermediate(container_factory):

    class DedicatedExtension(CallCollectorMixin, Extension):
        pass

    class SharedExtension(CallCollectorMixin, Extension):
        ext = DedicatedExtension()

    class SimpleInjection(CallCollectorMixin, InjectionProvider):
        ext = SharedExtension(shared=True)

    class Service(object):
        inj_1 = SimpleInjection()
        inj_2 = SimpleInjection()

    container = container_factory(Service, {})
    container.start()

    assert len(container.dependencies) == 4
    assert len(CallCollectorMixin.calls[SimpleInjection]['start']) == 2
    assert len(CallCollectorMixin.calls[SharedExtension]['start']) == 1
    assert len(CallCollectorMixin.calls[DedicatedExtension]['start']) == 1


def test_shared_extension_uniqueness(container_factory):

    class SharedExtension(CallCollectorMixin, Extension):
        pass

    class SimpleInjection(CallCollectorMixin, InjectionProvider):
        ext = SharedExtension(shared=True)

    class Service(object):
        inj_1 = SimpleInjection()
        inj_2 = SimpleInjection()

    c1 = container_factory(Service, {})
    c2 = container_factory(Service, {})

    # extension declarations are identical between containers
    assert c1.service_cls.inj_1.ext == c2.service_cls.inj_1.ext

    # extension instances are different between containers
    shared_1 = get_dependency(c1, SharedExtension)
    shared_2 = get_dependency(c2, SharedExtension)
    assert shared_1 is not shared_2
