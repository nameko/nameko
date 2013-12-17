from mock import Mock

from nameko.testing.service.unit import instance_factory


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
