# websocket_rpc.py

from nameko.web.websocket import rpc, WebSocketHubProvider

class WebsocketRpc:
    name = "websocket_rpc_service"

    websocket_hub = WebSocketHubProvider()

    @rpc
    def echo(self, socket_id, value):
        return value
