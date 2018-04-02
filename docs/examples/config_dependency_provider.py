from nameko.dependency_providers import Config
from nameko.web.handlers import http


class Service:

    name = "test_config"

    config = Config()

    @property
    def foo_enabled(self):
        return self.config.get('FOO_FEATURE_ENABLED', False)

    @http('GET', '/foo')
    def foo(self, request):
        if not self.foo_enabled:
            return 403, "FeatureNotEnabled"

        return 'foo'
