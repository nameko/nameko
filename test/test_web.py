import json
from nameko.web.handlers import http


class ExampleService(object):

    @http('GET', '/foo/<int:bar>')
    def do_foo(self, bar):
        return {'value': bar}

    @http('POST', '/post')
    def do_post(self, value):
        return {'value': value}


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
