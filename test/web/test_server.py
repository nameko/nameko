from mock import patch
import pytest
import socket
from eventlet import wsgi
from werkzeug.contrib.fixers import ProxyFix

from nameko.exceptions import ConfigurationError
from nameko.web.handlers import http, HttpRequestHandler
from nameko.web.server import (
    BaseHTTPServer,
    parse_address,
    WebServer,
    HttpOnlyProtocol
)


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
    container_factory, web_config, web_config_port, web_session
):
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
    container_factory, web_config, web_config_port, web_session
):
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


def test_adding_middleware_with_get_wsgi_app(container_factory, web_config):

    class CustomWebServer(WebServer):
        def get_wsgi_app(self):
            # get the original WSGI app that processes http requests
            app = super(CustomWebServer, self).get_wsgi_app()
            # apply the ProxyFix middleware as an example
            return ProxyFix(app, num_proxies=1)

    class CustomHttpRequestHandler(HttpRequestHandler):
        server = CustomWebServer()

    http = CustomHttpRequestHandler.decorator

    class CustomServerExampleService(object):
        name = 'customserverservice'

        @http('GET', '/')
        def do_index(self, request):
            return ''

    container = container_factory(CustomServerExampleService, web_config)
    with patch.object(CustomWebServer, 'get_wsgi_server') as get_wsgi_server:
        container.start()

    wsgi_app = get_wsgi_server.call_args[0][1]
    assert isinstance(wsgi_app, ProxyFix)


def test_custom_wsgi_server_is_used(
    container_factory, web_config, web_config_port, web_session
):
    def custom_wsgi_app(environ, start_response):
        start_response('200 OK', [])
        return 'Override'

    class CustomWebServer(WebServer):
        def get_wsgi_server(
            self, sock, wsgi_app, protocol=HttpOnlyProtocol, debug=False
        ):
            return wsgi.Server(
                sock,
                sock.getsockname(),
                custom_wsgi_app,
                protocol=protocol,
                debug=debug
            )

    class CustomHttpRequestHandler(HttpRequestHandler):
        server = CustomWebServer()

    http = CustomHttpRequestHandler.decorator

    class CustomServerExampleService(object):
        name = 'customserverservice'

        @http('GET', '/')
        def do_index(self, request):
            return ''

    container = container_factory(CustomServerExampleService, web_config)
    container.start()

    assert web_session.get('/').text == 'Override'
