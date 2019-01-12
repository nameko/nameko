import pytest
from mock import call, patch

from nameko import config
from nameko.dependency_providers import Config
from nameko.rpc import rpc
from nameko.testing.services import entrypoint_hook


class TestConfig:

    @pytest.fixture
    def service_cls(self):
        class Service:
            name = "test_x"
            config = Config()

            @rpc
            def foo(self):
                return self.config.get("FOO")

        return Service

    @pytest.mark.usefixtures("rabbit_config")
    @patch("nameko.dependency_providers.warnings.warn")
    def test_get_config_value(
        self, warn, rabbit_config, container_factory, service_cls
    ):
        with config.patch({"FOO": "bar"}):

            container = container_factory(service_cls)
            container.start()

            with entrypoint_hook(container, "foo") as foo:
                assert foo() == "bar"

        assert (
            warn.call_args ==
            call("Use ``nameko.config`` instead.", DeprecationWarning)
        )
