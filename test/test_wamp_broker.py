import pytest

from nameko.extensions.entrypoints.wamp.connection import WampWebSocket
from nameko.extensions.entrypoints.wamp.rpc import wamp_rpc

from nameko.testing.services import entrypoint_waiter


URL = "ws://staging-crossbar-io-002.tintofs.com:8090/ws"


@pytest.fixture
def websocket():
    ws = WampWebSocket()
    ws.connect(URL)
    return ws


@pytest.fixture
def message(websocket):
    ws = websocket
    ws.send("hello world")
    print('send message')


class ExampleService(object):
    name = 'nameko_example'

    @wamp_rpc
    def handler(self):
        print('test handler called')


def test_wamp_broker(container_factory, wamp_config, message):
    container = container_factory(ExampleService, {})
    container.start()

    with entrypoint_waiter(container, 'handler', timeout=10):
        pass
