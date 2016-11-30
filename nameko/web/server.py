import os
import re
import socket
import sys
from functools import partial

from six.moves.urllib.parse import urlparse

import eventlet
from eventlet import wsgi
from eventlet.green import socket
from eventlet.support import get_errno
from eventlet.wsgi import BROKEN_SOCK, BaseHTTPServer, HttpProtocol
from werkzeug.exceptions import HTTPException
from werkzeug.routing import Map
from werkzeug.wrappers import Request

from nameko.constants import WEB_SERVER_CONFIG_KEY
from nameko.exceptions import ConfigurationError
from nameko.extensions import ProviderCollector, SharedExtension


class HttpOnlyProtocol(HttpProtocol):
    # identical to HttpProtocol.finish, except greenio.shutdown_safe
    # is removed. it's only needed to ssl sockets which we don't support
    # this is a workaround until
    # https://bitbucket.org/eventlet/eventlet/pull-request/42
    # or something like it lands
    def finish(self):
        try:
            # patched in depending on python version; confuses pylint
            # pylint: disable=E1101
            BaseHTTPServer.BaseHTTPRequestHandler.finish(self)
        except socket.error as e:
            # Broken pipe, connection reset by peer
            if get_errno(e) not in BROKEN_SOCK:
                raise
        self.connection.close()


def socket_from_uri(web_server_uri):
    address = None
    uri = urlparse(web_server_uri)
    if uri.scheme == 'tcp':
        _socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        address = (uri.hostname, uri.port)
    elif uri.scheme == 'unix':
        _socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            os.remove(uri.path)
        except OSError:
            pass
        address = uri.path
    elif uri.scheme == 'fd':
        _socket = socket.fromfd(int(uri.netloc), socket.AF_INET, socket.SOCK_STREAM)
    # from eventlet.listen
    if sys.platform[:3] != "win":
        _socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if hasattr(socket, 'SO_REUSEPORT'):
        # NOTE(zhengwei): linux kernel >= 3.9
        _socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

    if address:
        _socket.bind(address)

    if uri.query:
        options = dict(item.split('=') for item in uri.query.split('&'))
    else:
        options = {}
    # backlog default from eventlet.listen
    options.setdefault('backlog', 50)
    _socket.listen(int(options['backlog']))
    return _socket


class WebServer(ProviderCollector, SharedExtension):
    """A SharedExtension that wraps a WSGI interface for processing HTTP
    requests.

    WebServer can be subclassed to add additional WSGI functionality through
    overriding the get_wsgi_server and get_wsgi_app methods.
    """

    def __init__(self):
        super(WebServer, self).__init__()
        self._gt = None
        self._serv = None
        self._socket = None
        self._starting = False
        self._is_accepting = True

    @property
    def socket(self):
        if self._socket is None:
            web_server_uri = self.container.config.get(
                WEB_SERVER_CONFIG_KEY, None)
            if web_server_uri is None:
                # backcompat
                web_server_uri = 'tcp://'+ self.container.config.get(
                    'WEB_SERVER_ADDRESS', '0.0.0.0:8000')
            self._socket = socket_from_uri(web_server_uri)
        return self._socket

    def run(self):
        while self._is_accepting:
            sock, addr = self.socket.accept()
            sock.settimeout(self._serv.socket_timeout)
            self.container.spawn_managed_thread(
                partial(self.process_request, sock, addr)
            )

    def process_request(self, sock, address):
        try:
            self._serv.process_request((sock, address))
        except OSError as exc:
            # OSError("raw readinto() returned invalid length")
            # can be raised when a client disconnects very early as a result
            # of an eventlet bug: https://github.com/eventlet/eventlet/pull/353
            # See https://github.com/onefinestay/nameko/issues/368
            if "raw readinto() returned invalid length" in str(exc):
                return
            raise

    def start(self):
        if not self._starting:
            self._starting = True
            self._serv = self.get_wsgi_server(self.socket, self.get_wsgi_app())
            self._gt = self.container.spawn_managed_thread(self.run)

    def get_wsgi_app(self):
        """Get the WSGI application used to process requests.

        This method can be overriden to apply WSGI middleware or replace
        the WSGI application all together.
        """
        return WsgiApp(self)

    def get_wsgi_server(
        self, sock, wsgi_app, protocol=HttpOnlyProtocol, debug=False
    ):
        """Get the WSGI server used to process requests."""
        return wsgi.Server(
            sock,
            sock.getsockname(),
            wsgi_app,
            protocol=protocol,
            debug=debug
        )

    def stop(self):
        self._is_accepting = False
        self._gt.kill()
        self.socket.close()
        super(WebServer, self).stop()

    def make_url_map(self):
        url_map = Map()
        for provider in self._providers:
            rule = provider.get_url_rule()
            rule.endpoint = provider
            url_map.add(rule)
        return url_map

    def context_data_from_headers(self, request):
        return {}


class WsgiApp(object):

    def __init__(self, server):
        self.server = server
        self.url_map = server.make_url_map()

    def __call__(self, environ, start_response):
        # set to shallow mode so that nobody can read request data in
        # until unset.  This allows the connection to be upgraded into
        # a bidirectional websocket connection later if we need in all
        # cases.  The regular request handling code unsets this flag
        # automatically.
        #
        # If we would not do this and some code would access the form data
        # before that point, we might deadlock outselves because the
        # browser is no longer reading from our socket at this point in
        # time.
        request = Request(environ, shallow=True)
        adapter = self.url_map.bind_to_environ(environ)
        try:
            provider, values = adapter.match()
            request.path_values = values
            rv = provider.handle_request(request)
        except HTTPException as exc:
            rv = exc
        return rv(environ, start_response)
