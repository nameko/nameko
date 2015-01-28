from mock import Mock
import pytest
from requests.exceptions import ConnectionError

from nameko.containers import ServiceContainer
from nameko.web.server import WebServer


def test_foo(web_session):
    with pytest.raises(ConnectionError):
        web_session.get('/')

    container = Mock(spec=ServiceContainer)
    container.shared_extensions = {}
    container.config = {}

    server = WebServer()
    # server.bind(container)
    server.container = container
    server.start()
