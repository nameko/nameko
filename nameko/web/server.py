import re
from collections import namedtuple
from functools import partial
from logging import getLogger

from werkzeug.exceptions import HTTPException
from werkzeug.routing import Map
from werkzeug.wrappers import Request

import nameko.concurrency
from nameko import config
from nameko.constants import WEB_SERVER_CONFIG_KEY
from nameko.exceptions import ConfigurationError
from nameko.extensions import ProviderCollector, SharedExtension


BindAddress = namedtuple("BindAddress", ['address', 'port'])


def parse_address(address_string):
    address_re = re.compile(r'^((?P<address>[^:]+):)?(?P<port>\d+)$')
    match = address_re.match(address_string)
    if match is None:
        raise ConfigurationError(
            'Misconfigured bind address `{}`. '
            'Should be `[address:]port`'.format(address_string)
        )
    address = match.group('address') or ''
    port = int(match.group('port'))
    return BindAddress(address, port)


class WebServer(ProviderCollector, SharedExtension):
    """A SharedExtension that wraps a WSGI interface for processing HTTP
    requests.

    WebServer can be subclassed to add additional WSGI functionality through
    overriding the `get_wsgi_server` and `get_wsgi_app` methods.
    """

    def __init__(self):
        super(WebServer, self).__init__()
        self._gt = None
        self._sock = None
        self._serv = None
        self._starting = False
        self._is_accepting = True

    @property
    def bind_addr(self):
        address_str = config.get(
            WEB_SERVER_CONFIG_KEY, '0.0.0.0:8000')
        return parse_address(address_str)

    def run(self):
        while self._is_accepting:
            sock, addr = self._sock.accept()
            sock.settimeout(self._serv.socket_timeout)
            self.container.spawn_managed_thread(
                partial(self.process_request, sock, addr)
            )

    def process_request(self, sock, address):
        nameko.concurrency.process_wsgi_request(self._serv, sock, address)

    def start(self):
        if not self._starting:
            self._starting = True
            self._sock = nameko.concurrency.listen(self.bind_addr)
            # work around https://github.com/celery/kombu/issues/838
            self._sock.settimeout(None)
            self._serv = self.get_wsgi_server(self._sock, self.get_wsgi_app())
            self._gt = self.container.spawn_managed_thread(self.run)

    def get_wsgi_app(self):
        """Get the WSGI application used to process requests.

        This method can be overridden to apply WSGI middleware or replace
        the WSGI application all together.
        """
        return WsgiApp(self)

    def get_wsgi_server(
        self, sock, wsgi_app, protocol=nameko.concurrency.HttpOnlyProtocol, debug=False
    ):
        """Get the WSGI server used to process requests."""
        return nameko.concurrency.get_wsgi_server(
            sock=sock,
            wsgi_app=wsgi_app,
            protocol=protocol,
            debug=debug,
            log=getLogger(__name__)
        )

    def stop(self):
        self._is_accepting = False
        self._gt.kill()
        self._sock.close()
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
