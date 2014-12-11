from nameko.web.handlers import http
from nameko.web.websocket import websocket_hub, wsrpc

from nameko.exceptions import RemoteError


class ExampleService(object):
    websocket = websocket_hub()

    @http('GET', '/foo/<int:bar>')
    def do_foo(self, bar):
        return {'value': bar}

    @http('POST', '/post')
    def do_post(self, value):
        return {'value': value}

    @wsrpc
    def subscribe(self, socket_id):
        self.websocket.subscribe(socket_id, 'test_channel')
        return 'subscribed!'

    @wsrpc
    def broadcast(self, socket_id, value):
        self.websocket.broadcast('test_channel', 'test_message', {
            'value': value,
        })
        return 'broadcast!'


def test_simple_rpc(container_factory, web_config, web_session):
    container = container_factory(ExampleService, web_config)
    container.start()

    rv = web_session.get('/foo/42')
    assert rv.json() == {'data': {'value': 42}, 'success': True}

    rv = web_session.get('/foo/something')
    assert rv.status_code == 404


def test_post_rpc(container_factory, web_config, web_session):
    container = container_factory(ExampleService, web_config)
    container.start()

    rv = web_session.post('/post', json={
        'value': 23,
    })
    assert rv.json() == {'data': {'value': 23}, 'success': True}

    rv = web_session.post('/post', json={
        'value': 23,
        'extra': []
    })
    resp = rv.json()
    assert rv.status_code == 400
    assert not resp['success']
    assert resp['error']['type'] == 'nameko.exceptions.IncorrectSignature'


def test_websockets(container_factory, web_config, websocket):
    container = container_factory(ExampleService, web_config)
    container.start()

    ws = websocket()
    assert ws.rpc('subscribe') == 'subscribed!'
    assert ws.rpc('broadcast', value=42) == 'broadcast!'
    try:
        assert ws.rpc('broadcast')
    except RemoteError as e:
        assert e.exc_type == 'nameko.exceptions.IncorrectSignature'
    else:
        assert False, 'expected a remote error but got nothing'

    assert ws.wait_for_event('test_message') == ('test_message', {
        'value': 42,
    })
