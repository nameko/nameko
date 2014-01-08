from mock import Mock

from nameko.dependencies import InjectionProvider
from nameko.testing.dependencies import patch_injection_provider


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
