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
        "--amqp-uri", action="store", dest='AMQP_URI',
        default='amqp://guest:guest@localhost:5672/:random:',
        help=("The AMQP-URI to connect to rabbit with."))

    parser.addoption(
        "--rabbit-ctl-uri", action="store", dest='RABBIT_CTL_URI',
        default='http://guest:guest@localhost:15672',
        help=("The URI for rabbit's management API."))


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
    from nameko.constants import AMQP_URI_CONFIG_KEY
    return {
        AMQP_URI_CONFIG_KEY: ""
    }


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
    return rabbit.Client(config.getoption('RABBIT_CTL_URI'))


@pytest.yield_fixture()
def rabbit_config(request, rabbit_manager):
    import itertools
    import random
    import string
    import time
    from kombu import pools
    from six.moves.urllib.parse import urlparse  # pylint: disable=E0401
    from nameko.testing.utils import get_rabbit_connections

    amqp_uri = request.config.getoption('AMQP_URI')

    uri = urlparse(amqp_uri)
    username = uri.username
    vhost = uri.path[1:]

    use_random_vost = (vhost == ":random:")

    if use_random_vost:
        vhost = "test_{}".format(
            "".join(random.choice(string.ascii_lowercase) for _ in range(10))
        )
        amqp_uri = "{}://{}/{}".format(uri.scheme, uri.netloc, vhost)
        rabbit_manager.create_vhost(vhost)
        rabbit_manager.set_vhost_permissions(vhost, username, '.*', '.*', '.*')

    conf = {
        'AMQP_URI': amqp_uri,
        'username': username,
        'vhost': vhost
    }

    yield conf

    pools.reset()  # close connections in pools

    def retry(fn):
        """ Barebones retry decorator
        """
        def wrapper():
            max_retries = 3
            delay = 1
            exceptions = RuntimeError

            counter = itertools.count()
            while True:
                try:
                    return fn()
                except exceptions:
                    if next(counter) == max_retries:
                        raise
                    time.sleep(delay)
        return wrapper

    @retry
    def check_connections():
        """ Raise a runtime error if the test leaves any connections open.

        Allow a few retries because the rabbit api is eventually consistent.
        """
        connections = get_rabbit_connections(conf['vhost'], rabbit_manager)
        open_connections = [
            conn for conn in connections if conn['state'] != "closed"
        ]
        if open_connections:
            count = len(open_connections)
            names = ", ".join(conn['name'] for conn in open_connections)
            raise RuntimeError(
                "{} rabbit connection(s) left open: {}".format(count, names))
    try:
        check_connections()
    finally:
        if use_random_vost:
            rabbit_manager.delete_vhost(vhost)


@pytest.fixture
def ensure_cleanup_order(request):
    """ Ensure ``rabbit_config`` is invoked early if it's used by any fixture
    in ``request``.
    """
    if "rabbit_config" in request.funcargnames:
        request.getfuncargvalue("rabbit_config")


@pytest.yield_fixture
def container_factory(ensure_cleanup_order):
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
            c.stop()
        except:  # pragma: no cover
            pass


@pytest.yield_fixture
def runner_factory(ensure_cleanup_order):
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
            r.stop()
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
    import socket
    from nameko.constants import WEB_SERVER_CONFIG_KEY

    # find a port that's likely to be free
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[1]
    sock.close()

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
