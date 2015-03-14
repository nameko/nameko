import socket

import eventlet
import pytest

from nameko.testing.websocket import make_virtual_socket


@pytest.yield_fixture()
def web_config(rabbit_config):
    # find a port that's likely to be free
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[1]
    sock.close()

    cfg = rabbit_config
    cfg['WEB_SERVER_PORT'] = port
    cfg['WEB_SERVER_BASE_URL'] = '/test'
    yield cfg


@pytest.yield_fixture()
def web_session(web_config):
    from requests import Session
    from werkzeug.urls import url_join

    port = web_config['WEB_SERVER_PORT']
    base_url = web_config['WEB_SERVER_BASE_URL']

    class WebSession(Session):
        def request(self, method, url, *args, **kwargs):
            url = url_join('http://127.0.0.1:{}'.format(port), base_url + url)
            return Session.request(self, method, url, *args, **kwargs)

    sess = WebSession()
    with sess:
        yield sess


@pytest.yield_fixture()
def websocket(web_config):
    active_sockets = []

    def socket_creator():
        ws_app, wait_for_sock = make_virtual_socket(
            '127.0.0.1', web_config['WEB_SERVER_PORT'])
        gr = eventlet.spawn(ws_app.run_forever)
        active_sockets.append((gr, ws_app))
        return wait_for_sock()

    try:
        yield socket_creator
    finally:
        for gr, ws_app in active_sockets:
            try:
                ws_app.close()
            except Exception:
                pass
            gr.kill()
