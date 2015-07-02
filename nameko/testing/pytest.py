""" pytest plugin serving as a default conftest for nameko projects.
"""
from __future__ import absolute_import

import eventlet
eventlet.monkey_patch()  # noqa (code before rest of imports)

import itertools
import logging
import socket
import sys

from kombu import pools
from mock import patch
import pytest

from nameko.containers import ServiceContainer
from nameko.runners import ServiceRunner
from nameko.testing import rabbit
from nameko.testing.utils import (
    reset_rabbit_vhost, reset_rabbit_connections,
    get_rabbit_connections, get_rabbit_config)
from nameko.testing.websocket import make_virtual_socket
from nameko.web.server import parse_address


def pytest_addoption(parser):
    parser.addoption(
        '--blocking-detection',
        action='store_true',
        dest='blocking_detection',
        default=False,
        help='turn on eventlet hub blocking detection')

    parser.addoption(
        "--log-level", action="store",
        default='DEBUG',
        help=("The logging-level for the test run."))

    parser.addoption(
        "--amqp-uri", action="store", dest='AMQP_URI',
        default='amqp://guest:guest@localhost:5672/nameko_test',
        help=("The AMQP-URI to connect to rabbit with."))

    parser.addoption(
        "--rabbit-ctl-uri", action="store", dest='RABBIT_CTL_URI',
        default='http://guest:guest@localhost:15672',
        help=("The URI for rabbit's management API."))


def pytest_configure(config):
    if config.option.blocking_detection:  # pragma: no cover
        from eventlet import debug
        debug.hub_blocking_detection(True)

    log_level = config.getoption('log_level')
    if log_level is not None:
        log_level = getattr(logging, log_level)
        logging.basicConfig(level=log_level, stream=sys.stderr)


@pytest.fixture
def empty_config(request):
    return {'AMQP_URI': ""}


@pytest.fixture(scope='session')
def rabbit_manager(request):
    config = request.config
    return rabbit.Client(config.getoption('RABBIT_CTL_URI'))


@pytest.yield_fixture()
def rabbit_config(request, rabbit_manager):
    amqp_uri = request.config.getoption('AMQP_URI')

    conf = get_rabbit_config(amqp_uri)

    reset_rabbit_connections(conf['vhost'], rabbit_manager)
    reset_rabbit_vhost(conf['vhost'], conf['username'], rabbit_manager)

    yield conf

    pools.reset()  # close connections in pools

    # raise a runtime error if the test leaves any connections lying around
    connections = get_rabbit_connections(conf['vhost'], rabbit_manager)
    if connections:  # pragma: no cover
        count = len(connections)
        raise RuntimeError("{} rabbit connection(s) left open.".format(count))


@pytest.yield_fixture
def container_factory(rabbit_config):

    all_containers = []

    def make_container(service_cls, config, worker_ctx_cls=None):
        container = ServiceContainer(service_cls, config, worker_ctx_cls)
        all_containers.append(container)
        return container

    yield make_container

    for c in all_containers:
        try:
            c.stop()
        except:  # pragma: no cover
            pass


@pytest.yield_fixture
def runner_factory(rabbit_config):

    all_runners = []

    def make_runner(config, *service_classes):
        runner = ServiceRunner(config)
        for service_cls in service_classes:
            runner.add_service(service_cls)
        all_runners.append(runner)
        return runner

    yield make_runner

    for r in all_runners:
        try:
            r.stop()
        except:  # pragma: no cover
            pass


@pytest.yield_fixture
def predictable_call_ids(request):
    with patch('nameko.containers.new_call_id', autospec=True) as get_id:
        get_id.side_effect = (str(i) for i in itertools.count())
        yield get_id


@pytest.yield_fixture()
def web_config(rabbit_config):
    # find a port that's likely to be free
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[1]
    sock.close()

    cfg = rabbit_config
    cfg['WEB_SERVER_ADDRESS'] = str(port)
    yield cfg


@pytest.fixture()
def web_config_port(web_config):
    return parse_address(web_config['WEB_SERVER_ADDRESS']).port


@pytest.yield_fixture()
def web_session(web_config_port):
    from requests import Session
    from werkzeug.urls import url_join

    class WebSession(Session):
        def request(self, method, url, *args, **kwargs):
            url = url_join('http://127.0.0.1:%d/' % web_config_port, url)
            return Session.request(self, method, url, *args, **kwargs)

    sess = WebSession()
    with sess:
        yield sess


@pytest.yield_fixture()
def websocket(web_config_port):
    active_sockets = []

    def socket_creator():
        ws_app, wait_for_sock = make_virtual_socket(
            '127.0.0.1', web_config_port)
        gr = eventlet.spawn(ws_app.run_forever)
        active_sockets.append((gr, ws_app))
        return wait_for_sock()

    try:
        yield socket_creator
    finally:
        for gr, ws_app in active_sockets:
            try:
                ws_app.close()
            except Exception:  # pragma: no cover
                pass
            gr.kill()
