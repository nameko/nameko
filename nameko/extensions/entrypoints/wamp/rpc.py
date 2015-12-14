from nameko.extensions import (
    Entrypoint, ProviderCollector, SharedExtension)
from nameko.web.websocket import WebSocketServer


URL = "ws://staging-crossbar-io-002.tintofs.com:8080/ws"


class WampWebSocketServer(SharedExtension, ProviderCollector):

    def __init__(self):
        super(WampWebSocketServer, self).__init__()
        self.conn = None

    def setup(self):
        print('setup WampWebSocketServer')
        from nameko.extensions.entrypoints.wamp.connection import WampWebSocket
        conn = WampWebSocket()
        conn.connect(URL)
        assert conn.connected

        self.conn = conn

    def stop(self):
        print('stop WampWebSocketServer')
        super(WampWebSocketServer, self).stop()
        self.conn.close()

    def handle_request(self, request):
        print('handle request')
        print request


class WampRpc(Entrypoint):
    server = WampWebSocketServer()

    def setup(self):
        print('setup WampRpc')
        self.server.register_provider(self)

    def stop(self):
        print('stop')
        self.server.unregister_provider(self)
        super(WampRpc, self).stop()

    def handle_message(self, socket_id, data, context_data):
        print('handle message')

    def handle_result(self, event, worker_ctx, result, exc_info):
        print('handle result')


wamp_rpc = WampRpc.decorator
