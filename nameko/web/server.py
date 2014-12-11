import eventlet
from eventlet import wsgi, support
from functools import partial

from werkzeug.wrappers import Request
from werkzeug.routing import Map
from werkzeug.exceptions import HTTPException

from nameko.dependencies import (
    ProviderCollector, DependencyProvider, DependencyFactory, dependency)


WEB_SERVER_HOST_CONFIG_KEY = 'WEB_SERVER_HOST'
WEB_SERVER_PORT_CONFIG_KEY = 'WEB_SERVER_PORT'


class Server(DependencyProvider, ProviderCollector):

    def __init__(self):
        super(Server, self).__init__()
        self._gt = None
        self._sock = None
        self._serv = None
        self._wsgi_app = None
        self._starting = False
        self._is_accepting = True

    @property
    def bind_addr(self):
        return (
            self.container.config.get(WEB_SERVER_HOST_CONFIG_KEY, ''),
            self.container.config.get(WEB_SERVER_PORT_CONFIG_KEY, 8000),
        )

    def run(self):
        while self._is_accepting:
            try:
                sock, addr = self._sock.accept()
            except wsgi.ACCEPT_EXCEPTIONS as e:
                if support.get_errno(e) not in wsgi.ACCEPT_ERRNO:
                    raise
            sock.settimeout(self._serv.socket_timeout)
            self.container.spawn_managed_thread(partial(
                self._serv.process_request, (sock, addr)),
                protected=True)

    def start(self):
        if not self._starting:
            self._starting = True
            self._wsgi_app = WsgiApp(self)
            self._sock = eventlet.listen(self.bind_addr)
            self._serv = wsgi.Server(self._sock,
                                     self._sock.getsockname(),
                                     self._wsgi_app,
                                     debug=False)
            self._gt = self.container.spawn_managed_thread(
                self.run, protected=True)

    def stop(self):
        self._is_accepting = False
        self._gt.kill()
        super(Server, self).stop()

    def make_url_map(self):
        map = Map()
        for provider in self._providers:
            rule = provider.get_url_rule()
            rule.endpoint = provider
            map.add(rule)
        return map

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
        except HTTPException as e:
            rv = e
        return rv(environ, start_response)


@dependency
def server():
    return DependencyFactory(Server)
