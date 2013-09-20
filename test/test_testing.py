from mock import Mock

from nameko.dependencies import AttributeDependency
from nameko.testing import patch_attr_dependency


def test_patch_attr_dependency():

    class TestProvider(AttributeDependency):
        def acquire_injection(self, worker_ctx):
            return 'before patch injection'

    attr = TestProvider()
    patch_attr = patch_attr_dependency(attr)

    # make sure patching does not happen until used as a contextmanager
    assert attr.acquire_injection(None) == 'before patch injection'

    with patch_attr as injected_mock:
        assert attr.acquire_injection(None) is injected_mock
        assert isinstance(injected_mock, Mock)
