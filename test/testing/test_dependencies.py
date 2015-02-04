from mock import Mock

from nameko.extensions import DependencyProvider
from nameko.testing.dependencies import patch_injection_provider


def test_patch_attr_dependency():

    class TestProvider(DependencyProvider):
        def get_dependency(self, worker_ctx):
            return 'before patch injection'

    attr = TestProvider()
    patch_attr = patch_injection_provider(attr)

    # make sure patching does not happen until used as a contextmanager
    assert attr.get_dependency(None) == 'before patch injection'

    with patch_attr as injected_mock:
        assert attr.get_dependency(None) is injected_mock
        assert isinstance(injected_mock, Mock)
