import pytest
from werkzeug.wrappers import Response

from nameko.web.handlers import http


class ExampleService(object):

    @http('GET', '/foo/<int:bar>')
    def do_foo(self, bar):
        return {'value': bar}

    @http('POST', '/post')
    def do_post(self, value):
        return {'value': value}

    @http('GET', '/custom')
    def do_custom(self):
        return Response('response')

    @http('GET', '/status_code')
    def do_status_code(self):
        return 201, 'created'

    @http('GET', '/headers')
    def do_headers(self):
        return 201, {'x-foo': 'bar'}, 'created'

    @http('GET', '/fail')
    def fail(self):
        raise ValueError('oops')


@pytest.fixture
def web_session(container_factory, web_config, web_session):
    container = container_factory(ExampleService, web_config)
    container.start()
    return web_session


def test_simple_rpc(web_session):
    rv = web_session.get('/foo/42')
    assert rv.json() == {'data': {'value': 42}, 'success': True}

    rv = web_session.get('/foo/something')
    assert rv.status_code == 404


def test_post_rpc(web_session):
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
    assert resp['error']['exc_path'] == 'nameko.exceptions.IncorrectSignature'


def test_post_bad_data(web_session):
    rv = web_session.post(
        '/post',
        data='foo: bar',
        headers={'content-type': 'application/json'},
    )
    assert rv.status_code == 400
    response = rv.json()
    assert not response['success']


def test_custom_response(web_session):
    rv = web_session.get('/custom')
    assert rv.content == 'response'


def test_custom_status_code(web_session):
    rv = web_session.get('/status_code')
    assert rv.json() == {'data': 'created', 'success': True}
    assert rv.status_code == 201


def test_custom_headers(web_session):
    rv = web_session.get('/headers')
    assert rv.json() == {'data': 'created', 'success': True}
    assert rv.status_code == 201
    assert rv.headers['x-foo'] == 'bar'


def test_broken_method(web_session):
    rv = web_session.get('/fail')
    assert rv.status_code == 500
