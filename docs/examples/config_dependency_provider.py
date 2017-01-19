from nameko.dependency_providers import Config
from nameko.rpc import rpc


class FeatureNotEnabled(Exception):
    pass


class Service:

    name = "test_config"

    config = Config()

    @property
    def foo_enabled(self):
        return self.config.get('FOO_FEATURE_ENABLED', False)

    @rpc
    def foo(self):
        if not self.foo_enabled:
            raise FeatureNotEnabled()

        return 'foo'
