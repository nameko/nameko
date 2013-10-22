from mock import Mock

from nameko.dependencies import InjectionProvider
from nameko.testing import patch_injection_provider
from nameko.testing.utils import AnyInstanceOf


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
