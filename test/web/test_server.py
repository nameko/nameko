import socket

import pytest
from mock import patch
from werkzeug.contrib.fixers import ProxyFix

import nameko.concurrency
from nameko.exceptions import ConfigurationError
from nameko.web.handlers import HttpRequestHandler, http
from nameko.web.server import WebServer, parse_address


class ExampleService(object):
    name = "exampleservice"

    @http('GET', '/')
    def do_index(self, request):
        return ''

    @http('GET', '/large')
    def do_large(self, request):
        # more than a buffer's worth
        return 'x' * (10**6)


@pytest.mark.usefixtures("web_config")
def test_broken_pipe(container_factory, web_config_port, web_session):
    container = container_factory(ExampleService)
    container.start()

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('127.0.0.1', web_config_port))
    s.sendall(b'GET /large \r\n\r\n')
    s.recv(10)
    s.close()  # break connection while there is still more data coming

    # server should still work
    assert web_session.get('/').text == ''


@pytest.mark.skipif(nameko.concurrency.mode != 'eventlet',
                    reason='Only tested for eventlet')
@pytest.mark.usefixtures("web_config")
def test_other_socket_error(container_factory, web_config_port, web_session):
    from eventlet.wsgi import BaseHTTPServer

    container = container_factory(ExampleService)
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


@pytest.mark.skipif(nameko.concurrency.mode != 'eventlet',
                    reason='Only tested for eventlet')
@pytest.mark.usefixtures("web_config")
def test_client_disconnect_os_error(
    container_factory, web_config_port, web_session
):
    """ Regression for https://github.com/nameko/nameko/issues/368
    """
    container = container_factory(ExampleService)
    container.start()

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('127.0.0.1', web_config_port))

    with patch.object(nameko.concurrency.HttpOnlyProtocol,
                      'handle_one_request') as handle:
        handle.side_effect = OSError('raw readinto() returned invalid length')
        s.sendall(b'GET / \r\n\r\n')
        s.recv(10)
        s.close()

    # server should still work
    assert web_session.get('/').text == ''


@pytest.mark.skipif(nameko.concurrency.mode != 'eventlet',
                    reason='Only tested for eventlet')
@pytest.mark.usefixtures("web_config")
def test_other_os_error(container_factory, web_config_port, web_session):
    """ Regression for https://github.com/nameko/nameko/issues/368
    """
    from eventlet.wsgi import BaseHTTPServer

    container = container_factory(ExampleService)
    container.start()

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('127.0.0.1', web_config_port))

    with patch.object(BaseHTTPServer.BaseHTTPRequestHandler, 'finish') as fin:
        fin.side_effect = OSError('boom')
        s.sendall(b'GET / \r\n\r\n')
        s.recv(10)
        s.close()

    # takes down container
    with pytest.raises(OSError) as exc:
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


@pytest.mark.usefixtures("web_config")
def test_adding_middleware_with_get_wsgi_app(container_factory):

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
            return ''  # pragma: no cover

    container = container_factory(CustomServerExampleService)
    with patch.object(CustomWebServer, 'get_wsgi_server') as get_wsgi_server:
        container.start()

    wsgi_app = get_wsgi_server.call_args[0][1]
    assert isinstance(wsgi_app, ProxyFix)


@pytest.mark.usefixtures("web_config")
def test_custom_wsgi_server_is_used(
    container_factory, web_config_port, web_session
):

    def custom_wsgi_app(environ, start_response):
        start_response('200 OK', [])
        return [b'Override']

    class CustomWebServer(WebServer):
        def get_wsgi_server(
                self, sock, wsgi_app,
                protocol=nameko.concurrency.HttpOnlyProtocol,
                debug=False
        ):
            return nameko.concurrency.get_wsgi_server(
                sock=sock,
                wsgi_app=custom_wsgi_app,
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
            return ''  # pragma: no cover

    container = container_factory(CustomServerExampleService)
    container.start()

    assert web_session.get('/').text == 'Override'
