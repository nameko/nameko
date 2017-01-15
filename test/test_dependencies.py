from mock import Mock, call, patch
import pytest

from nameko.dependencies import Config


class TestConfig:

    @pytest.yield_fixture(autouse=True)
    def patch_container(self):
        with patch.object(Config, 'container'):
            yield

    def test_get_dependency(self):
        config = Config()

        dependency = config.get_dependency(Mock(name='worker_ctx'))

        assert config.container.config.copy.return_value == dependency
        assert [call()] == config.container.config.copy.call_args_list
