import pytest

from nameko.contextdata import Language
from nameko.rpc import rpc
from nameko.testing.services import entrypoint_hook


class HelloService(object):
    """ Service under test
    """
    name = "hello_service"

    language = Language()

    @rpc
    def hello(self, name):
        greeting = "Hello"
        if self.language == "fr":
            greeting = "Bonjour"
        elif self.language == "de":
            greeting = "Gutentag"

        return "{}, {}!".format(greeting, name)


@pytest.mark.parametrize("language, greeting", [
    ("en", "Hello"),
    ("fr", "Bonjour"),
    ("de", "Gutentag"),
])
def test_hello_languages(language, greeting, container_factory, rabbit_config):

    container = container_factory(HelloService, rabbit_config)
    container.start()

    context_data = {'language': language}
    with entrypoint_hook(container, 'hello', context_data) as hook:
        assert hook("Matt") == "{}, Matt!".format(greeting)
