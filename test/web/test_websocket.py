import errno
import json
import socket

import eventlet
from eventlet.event import Event
import pytest

from nameko.exceptions import (
    MethodNotFound, RemoteError, deserialize, MalformedRequest)
from nameko.web.websocket import WebSocketHubProvider, rpc
from nameko.testing.services import get_extension, dummy, entrypoint_hook
from nameko.testing.websocket import make_virtual_socket


class ExampleService(object):
    name = "exampleservice"

    websocket_hub = WebSocketHubProvider()

    @rpc
    def subscribe(self, socket_id):
        self.websocket_hub.subscribe(socket_id, 'test_channel')
        return 'subscribed!'

    @rpc
    def unsubscribe(self, socket_id):
        self.websocket_hub.unsubscribe(socket_id, 'test_channel')
        return 'unsubscribed!'

    @rpc
    def list_subscriptions(self, socket_id):
        return self.websocket_hub.get_subscriptions(socket_id)

    @dummy
    def broadcast(self, value):
        self.websocket_hub.broadcast('test_channel', 'test_message', {
            'value': value,
        })

    @dummy
    def unicast(self, target_socket_id, value):
        status = self.websocket_hub.unicast(target_socket_id, 'test_message', {
            'value': value,
        })
        return status


def get_message(ws):
    # matches the broadcast rpc call
    event_type, event_data = ws.wait_for_event('test_message')
    assert event_type == 'test_message'
    assert list(event_data) == ['value']
    return event_data['value']


@pytest.yield_fixture
def container(container_factory, web_config):
    container = container_factory(ExampleService, web_config)
    container.start()
    yield container


def test_pub_sub(container, websocket):
    ws = websocket()
    assert ws.rpc('subscribe') == 'subscribed!'
    with entrypoint_hook(container, 'broadcast') as broadcast:
        broadcast(value=42)

    assert get_message(ws) == 42


def test_resubscribe(container, websocket):
    ws = websocket()
    ws.rpc('subscribe')
    ws.rpc('subscribe')

    with entrypoint_hook(container, 'broadcast') as broadcast:
        broadcast(value=42)
    assert get_message(ws) == 42


def test_multiple_calls(container, websocket):
    ws = websocket()
    ws.rpc('subscribe')
    count = 2
    for value in range(count):
        with entrypoint_hook(container, 'broadcast') as broadcast:
            broadcast(value=value)

    for value in range(count):
        assert get_message(ws) == value


def test_unsubscribe(container, websocket):
    ws = websocket()
    ws.rpc('subscribe')
    with entrypoint_hook(container, 'broadcast') as broadcast:
        broadcast(value=42)
    assert get_message(ws) == 42
    ws.rpc('unsubscribe')

    with entrypoint_hook(container, 'broadcast') as broadcast:
        broadcast(value=42)
    with eventlet.Timeout(.1, exception=False):
        assert get_message(ws) == 42


def test_unsubscribe_noop(container, websocket):
    ws = websocket()
    ws.rpc('unsubscribe')

    with entrypoint_hook(container, 'broadcast') as broadcast:
        broadcast(value=42)
    with eventlet.Timeout(.1, exception=False):
        assert get_message(ws) == 42


def test_multiple_subscribers(container, websocket):
    ws1 = websocket()
    ws2 = websocket()

    ws1.rpc('subscribe')
    ws2.rpc('subscribe')

    with entrypoint_hook(container, 'broadcast') as broadcast:
        broadcast(value=42)
    assert get_message(ws1) == 42
    assert get_message(ws2) == 42


def test_method_not_found(container, websocket):
    ws = websocket()
    with pytest.raises(MethodNotFound):
        ws.rpc('unknown')


def test_list_subscriptions(container, websocket):
    ws = websocket()
    assert ws.rpc('list_subscriptions') == []
    ws.rpc('subscribe')
    assert ws.rpc('list_subscriptions') == ['test_channel']


def test_unicast(container, websocket):
    ws = websocket()
    _, connected_data = ws.wait_for_event('connected')
    socket_id = connected_data['socket_id']
    with entrypoint_hook(container, 'unicast') as unicast:
        assert unicast(target_socket_id=socket_id, value=42)
    assert get_message(ws) == 42


def test_unicast_unknown(container):
    with entrypoint_hook(container, 'unicast') as unicast:
        assert not unicast(target_socket_id=0, value=42)


def test_connection_not_found(container, websocket):
    hub = get_extension(container, WebSocketHubProvider)
    ws = websocket()
    hub.server.sockets.clear()

    # doesn't need to be known
    assert ws.rpc('unsubscribe') == 'unsubscribed!'

    with pytest.raises(RemoteError) as exc:
        ws.rpc('subscribe')
    assert exc.value.exc_type == 'ConnectionNotFound'


def test_badly_encoded_data(container, web_config_port):
    ws_app, wait_for_sock = make_virtual_socket('127.0.0.1', web_config_port)

    gt = eventlet.spawn(ws_app.run_forever)
    wait_for_sock()
    result = Event()

    def on_message(ws, message):
        response = json.loads(message)
        assert not response['success']
        exc = deserialize(response['error'])
        result.send_exception(exc)
    ws_app.on_message = on_message
    ws_app.send('foo: bar')

    with pytest.raises(MalformedRequest) as exc:
        result.wait()
        assert 'Invalid JSON data' in str(exc)

    ws_app.close()
    gt.kill()


def test_websocket_helper_error(websocket):
    with pytest.raises(socket.error) as exc:
        websocket()
    assert exc.value.errno == errno.ECONNREFUSED


def test_client_closing_connection(container, web_config_port):
    ws_app, wait_for_sock = make_virtual_socket('127.0.0.1', web_config_port)

    gt = eventlet.spawn(ws_app.run_forever)
    wait_for_sock()

    wait_for_close = Event()

    def on_close(ws):
        wait_for_close.send(None)

    ws_app.on_close = on_close

    ws_app.send(b'\xff\x00')  # "Close the connection" packet.
    wait_for_close.wait()

    ws_app.close()
    gt.kill()

    assert container.stop() is None
