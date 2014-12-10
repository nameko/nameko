import json
import uuid

from functools import partial
from logging import getLogger

from eventlet.websocket import WebSocketWSGI
from eventlet.event import Event

from werkzeug.routing import Rule

from nameko.exceptions import MethodNotFound
from nameko.web.exceptions import ConnectionNotFound
from nameko.web.protocol import JsonProtocol
from nameko.web.server import server
from nameko.dependencies import (
    CONTAINER_SHARED, ProviderCollector, DependencyProvider,
    InjectionProvider, EntrypointProvider, DependencyFactory, dependency,
    injection, entrypoint)


_log = getLogger(__name__)


class Connection(object):

    def __init__(self, socket_id, context_data):
        self.socket_id = socket_id
        self.context_data = context_data
        self.subscriptions = set()


class WebSocketServer(DependencyProvider, ProviderCollector):
    wsgi_server = server(shared=CONTAINER_SHARED)
    protocol = JsonProtocol()

    def __init__(self):
        super(WebSocketServer, self).__init__()
        self.sockets = {}

    def get_url_rule(self):
        return Rule('/ws', methods=['GET'])

    def handle_request(self, request):
        try:
            return self.websocket_mainloop()
        except Exception:
            _log.error('websocket handling failed', exc_info=True)
            raise

    def websocket_mainloop(self):
        def handler(ws):
            socket_id, context_data = self.add_websocket(ws)
            try:
                while 1:
                    rv = self.handle_websocket_request(
                        socket_id, context_data, ws.wait())
                    ws.send(rv)
            finally:
                self.remove_socket(socket_id)
        return WebSocketWSGI(handler)

    def handle_websocket_request(self, socket_id, context_data, raw_req):
        try:
            method, data = self.protocol.deserialize_ws_frame(raw_req)
            provider = self.get_provider_for_method(method)
            return self.protocol.serialize_result(
                provider.handle_message(socket_id, data, context_data),
                ws=True)
        except Exception as e:
            _log.error('websocket message error', exc_info=True)
            return self.protocol.serialize_result(
                self.protocol.expose_exception(e)[1], success=False,
                ws=True)

    def get_provider_for_method(self, method):
        for provider in self._providers:
            if isinstance(provider, WebSocketRpc) and provider.name == method:
                return provider
        raise MethodNotFound()

    def prepare(self):
        self.wsgi_server.register_provider(self)

    def stop(self):
        self.wsgi_server.unregister_provider(self)
        super(WebSocketServer, self).stop()

    def add_websocket(self, ws):
        socket_id = str(uuid.uuid4())
        context_data = {}
        self.sockets[socket_id] = (ws, context_data)
        return socket_id, context_data

    def remove_socket(self, socket_id):
        self.sockets.pop(socket_id, None)
        for provider in self._providers:
            if isinstance(provider, WebSocketHub):
                provider.cleanup_websocket(socket_id)


@dependency
def websocket_server():
    return DependencyFactory(WebSocketServer)


class WebSocketHub(InjectionProvider):
    server = websocket_server(shared=CONTAINER_SHARED)

    def __init__(self):
        super(WebSocketHub, self).__init__()
        self.hub_controller = None

    def prepare(self):
        self.hub_controller = WebSocketHubController(self.server)
        self.server.register_provider(self)

    def stop(self):
        self.server.unregister_provider(self)
        super(WebSocketHub, self).stop()

    def acquire_injection(self, worker_ctx):
        return self.hub_controller

    def cleanup_websocket(self, socket_id):
        con = self.hub_controller.connections.pop(socket_id, None)
        if con is not None:
            for channel in con.subscriptions:
                subs = self.hub_controller.subscriptions.get(channel)
                if subs:
                    subs.discard(channel)


class WebSocketHubController(object):

    def __init__(self, server):
        self._server = server
        self.connections = {}
        self.subscriptions = {}

    def get_connection(self, socket_id, create=True):
        rv = self.connections.get(socket_id)
        if rv is not None:
            return rv
        rv = self._server.sockets.get(socket_id)
        if rv is None:
            if not create:
                return None
            raise ConnectionNotFound(socket_id)
        _, context_data = rv
        self.connections[socket_id] = rv = Connection(socket_id, context_data)
        return rv

    def subscribe(self, socket_id, channel):
        con = self.get_connection(socket_id)
        self.subscriptions.setdefault(channel, set()).add(socket_id)
        con.subscriptions.add(channel)

    def unsubscribe(self, socket_id, channel):
        con = self.get_connection(socket_id, create=False)
        if con is not None:
            con.subscriptions.discard(channel)
        try:
            self.subscriptions[channel].discard(socket_id)
        except KeyError:
            pass

    def broadcast(self, channel, event, data):
        payload = self._server.protocol.serialize_event(event, data)
        for socket_id in self.subscriptions.get(channel, ()):
            rv = self._server.sockets.get(socket_id)
            if rv is not None:
                rv[0].send(payload)


class WebSocketRpc(EntrypointProvider):
    server = websocket_server(shared=CONTAINER_SHARED)

    def __init__(self):
        super(WebSocketRpc, self).__init__()

    def prepare(self):
        self.server.register_provider(self)

    def stop(self):
        self.server.unregister_provider(self)
        super(WebSocketRpc, self).stop()

    def handle_message(self, socket_id, data, context_data):
        self.check_signature((socket_id,), data)
        event = Event()
        self.container.spawn_worker(self, (socket_id,), data,
                                    context_data=context_data,
                                    handle_result=partial(
                                        self.handle_result, event))
        return event.wait()

    def handle_result(self, event, worker_ctx, result, exc_info):
        event.send(result)
        return result, exc_info


@injection
def websocket_hub():
    return DependencyFactory(WebSocketHub)


@entrypoint
def wsrpc():
    return DependencyFactory(WebSocketRpc)
