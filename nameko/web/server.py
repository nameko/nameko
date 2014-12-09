import eventlet
from eventlet import wsgi
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

    def make_url_map(self):
        map = Map()
        for provider in self._providers:
            for rule in provider.iter_url_rules():
                map.add(rule)
        return map

    def get_provider_for_endpoint(self, endpoint):
        service_name = self.container.service_name
        for provider in self._providers:
            if '%s.%s' % (service_name, provider.name) == endpoint:
                return provider


class WsgiApp(object):

    def __init__(self, server):
        self.server = server
        self.url_map = server.make_url_map()

    def __call__(self, environ, start_response):
        request = Request(environ)
        adapter = self.url_map.bind_to_environ(environ)
        try:
            endpoint, values = adapter.match()
            provider = self.server.get_provider_for_endpoint(endpoint)
            request.path_values = values
            rv = provider.handle_request(request)
        except HTTPException as e:
            rv = e
        return rv(environ, start_response)


@dependency
def server():
    return DependencyFactory(Server)
