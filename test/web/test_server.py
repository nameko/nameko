from mock import patch
import pytest
import socket

from nameko.exceptions import ConfigurationError
from nameko.web.handlers import http
from nameko.web.server import BaseHTTPServer, parse_address


class ExampleService(object):
    name = "exampleservice"

    @http('GET', '/')
    def do_index(self, request):
        return ''

    @http('GET', '/large')
    def do_large(self, request):
        # more than a buffer's worth
        return 'x' * (10**6)


def test_broken_pipe(
        container_factory, web_config, web_config_port,  web_session):
    container = container_factory(ExampleService, web_config)
    container.start()

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('127.0.0.1', web_config_port))
    s.sendall(b'GET /large \r\n\r\n')
    s.recv(10)
    s.close()  # break connection while there is still more data coming

    # server should still work
    assert web_session.get('/').text == ''


def test_other_error(
        container_factory, web_config, web_config_port, web_session):
    container = container_factory(ExampleService, web_config)
    container.start()

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('127.0.0.1', web_config_port))

    with patch.object(BaseHTTPServer.BaseHTTPRequestHandler, 'finish') as fin:
        fin.side_effect = socket.error('boom')
        s.sendall(b'GET / \r\n\r\n')
        s.recv(10)
        s.close()

    # takes down container
    with pytest.raises(socket.error) as exc:
        container.wait()
    assert 'boom' in str(exc)


@pytest.mark.parametrize(['source', 'result'], [
    ('8000', ('', 8000)),
    ('foo:8000', ('foo', 8000)),
    ('foo', None),
])
def test_parse_address(source, result):
    if result is None:
        with pytest.raises(ConfigurationError) as exc:
            parse_address(source)
        assert 'Misconfigured bind address' in str(exc)
        assert '`foo`' in str(exc)

    else:
        assert parse_address(source) == result
