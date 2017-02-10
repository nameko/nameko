from __future__ import absolute_import

import pytest


# all imports are inline to make sure they happen after eventlet.monkey_patch
# which is called in pytest_load_initial_conftests
# (calling monkey_patch at import time breaks the pytest capturemanager - see
#  https://github.com/eventlet/eventlet/pull/239)


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
        "--rabbit-host", action="store", dest='RABBIT_HOST',
        default='localhost',
        help=("Hostname of the RabbitMQ broker."))

    parser.addoption(
        "--rabbit-port", action="store", dest='RABBIT_PORT',
        default='5672',
        help=("AMQP port number on RabbitMQ broker."))

    parser.addoption(
        "--rabbit-user", action="store", dest='RABBIT_USER',
        default='guest',
        help=("RabbitMQ username."))

    parser.addoption(
        "--rabbit-pass", action="store", dest='RABBIT_PASS',
        default='guest',
        help=("RabbitMQ password."))

    parser.addoption(
        "--rabbit-api-uri", action="store", dest='RABBIT_API_URI',
        default='http://guest:guest@localhost:15672',
        help=("URI for RabbitMQ management interface."))


def pytest_load_initial_conftests():
    # make sure we monkey_patch before local conftests
    import eventlet
    eventlet.monkey_patch()


def pytest_configure(config):
    import logging
    import sys

    if config.option.blocking_detection:  # pragma: no cover
        from eventlet import debug
        debug.hub_blocking_detection(True)

    log_level = config.getoption('log_level')
    if log_level is not None:
        log_level = getattr(logging, log_level)
        logging.basicConfig(level=log_level, stream=sys.stderr)


@pytest.fixture(autouse=True)
def always_warn_for_deprecation():
    import warnings
    warnings.simplefilter('always', DeprecationWarning)


@pytest.fixture
def empty_config():
    return {}


@pytest.fixture
def mock_container(request, empty_config):
    from mock import create_autospec
    from nameko.constants import SERIALIZER_CONFIG_KEY, DEFAULT_SERIALIZER
    from nameko.containers import ServiceContainer

    container = create_autospec(ServiceContainer)
    container.config = empty_config
    container.config[SERIALIZER_CONFIG_KEY] = DEFAULT_SERIALIZER
    container.serializer = container.config[SERIALIZER_CONFIG_KEY]
    container.accept = [DEFAULT_SERIALIZER]
    return container


@pytest.fixture(scope='session')
def rabbit_manager(request):
    from nameko.testing import rabbit

    config = request.config
    return rabbit.Client(config.getoption('RABBIT_API_URI'))


@pytest.yield_fixture()
def rabbit_config(request, rabbit_manager, container_factory, runner_factory):
    """
    Having this fixture depend on `container_factory` and `runner_factory`
    ensures that those fixtures tear down after this one.

    By chance, this considerably speeds up tests that use those fixtures.
    The reason is that deleting the RabbitMQ vhost sends basic-cancel to any
    consumers, triggering the ConsumerMixin to perform its iteration cycle
    early rather than waiting for the timeout to fire on its socket read.
    """
    import random
    import string

    host = request.config.getoption('RABBIT_HOST')
    port = request.config.getoption('RABBIT_PORT')
    username = request.config.getoption('RABBIT_USER')
    password = request.config.getoption('RABBIT_PASS')

    vhost = "nameko_test_{}".format(
        "".join(random.choice(string.ascii_lowercase) for _ in range(10))
    )
    amqp_uri = "pyamqp://{}:{}@{}:{}/{}".format(
        username, password, host, port, vhost
    )
    rabbit_manager.create_vhost(vhost)
    rabbit_manager.set_vhost_permissions(vhost, username, '.*', '.*', '.*')

    conf = {
        'AMQP_URI': amqp_uri,
        'username': username,
        'vhost': vhost
    }

    yield conf
    rabbit_manager.delete_vhost(vhost)


@pytest.yield_fixture
def container_factory():
    from nameko.containers import get_container_cls
    import warnings

    all_containers = []

    def make_container(service_cls, config, worker_ctx_cls=None):

        container_cls = get_container_cls(config)

        if worker_ctx_cls is not None:
            warnings.warn(
                "The constructor of `container_factory` has changed. "
                "The `worker_ctx_cls` kwarg is now deprecated. See CHANGES, "
                "Version 2.4.0 for more details.", DeprecationWarning
            )

        container = container_cls(service_cls, config, worker_ctx_cls)
        all_containers.append(container)
        return container

    yield make_container

    for c in all_containers:
        try:
            c.kill()
        except:  # pragma: no cover
            pass


@pytest.yield_fixture
def runner_factory():
    from nameko.runners import ServiceRunner

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
            r.kill()
        except:  # pragma: no cover
            pass


@pytest.yield_fixture
def predictable_call_ids(request):
    import itertools
    from mock import patch

    with patch('nameko.containers.new_call_id', autospec=True) as get_id:
        get_id.side_effect = (str(i) for i in itertools.count())
        yield get_id


@pytest.fixture()
def web_config(empty_config):
    from nameko.constants import WEB_SERVER_CONFIG_KEY
    from nameko.testing.utils import find_free_port

    port = find_free_port()

    cfg = empty_config
    cfg[WEB_SERVER_CONFIG_KEY] = str(port)
    return cfg


@pytest.fixture()
def web_config_port(web_config):
    from nameko.constants import WEB_SERVER_CONFIG_KEY
    from nameko.web.server import parse_address
    return parse_address(web_config[WEB_SERVER_CONFIG_KEY]).port


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
    import eventlet
    from nameko.testing.websocket import make_virtual_socket

    active_sockets = []

    def socket_creator():
        ws_app, wait_for_sock = make_virtual_socket(
            '127.0.0.1', web_config_port)
        gr = eventlet.spawn(ws_app.run_forever)
        active_sockets.append((gr, ws_app))
        socket = wait_for_sock()
        socket.app = ws_app
        return socket

    try:
        yield socket_creator
    finally:
        for gr, ws_app in active_sockets:
            try:
                ws_app.close()
            except Exception:  # pragma: no cover
                pass
            gr.kill()
