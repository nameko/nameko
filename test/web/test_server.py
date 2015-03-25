from mock import patch
import pytest
import socket

from nameko.web.handlers import http
from nameko.web.server import BaseHTTPServer


class ExampleService(object):
    name = "exampleservice"

    @http('GET', '/')
    def do_index(self, req):
        return ''

    @http('GET', '/large')
    def do_large(self, req):
        # more than a buffer's worth
        return 'x' * (10**6)


def test_broken_pipe(container_factory, web_config, web_session):
    container = container_factory(ExampleService, web_config)
    container.start()

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('127.0.0.1', web_config['WEB_SERVER_PORT']))
    s.sendall('GET /large \r\n\r\n')
    s.recv(10)
    s.close()  # break connection while there is still more data coming

    # server should still work
    assert web_session.get('/').text == ''


def test_other_error(container_factory, web_config, web_session):
    container = container_factory(ExampleService, web_config)
    container.start()

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('127.0.0.1', web_config['WEB_SERVER_PORT']))

    with patch.object(BaseHTTPServer.BaseHTTPRequestHandler, 'finish') as fin:
        fin.side_effect = socket.error('boom')
        s.sendall('GET / \r\n\r\n')
        s.recv(10)
        s.close()

    # takes down container
    with pytest.raises(socket.error) as exc:
        container.wait()
    assert 'boom' in str(exc)
