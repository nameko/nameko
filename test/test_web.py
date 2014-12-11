from nameko.web.handlers import http


class ExampleService(object):

    @http('GET', '/foo/<int:bar>')
    def do_foo(self, bar):
        return {'value': bar}


def test_simple(container_factory, web_config, web_session):
    container = container_factory(ExampleService, web_config)
    container.start()

    rv = web_session.get('/foo/42')
    assert rv.json() == {'data': {'value': 42}, 'success': True}

    rv = web_session.get('/foo/something')
    assert rv.status_code == 404
