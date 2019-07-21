from __future__ import absolute_import

import pytest


# all imports are inline to make sure they happen after eventlet.monkey_patch
# which is called in pytest_load_initial_conftests
# (calling monkey_patch at import time breaks the pytest capturemanager - see
#  https://github.com/eventlet/eventlet/pull/239)


def parse_config_option(text):
    import yaml
    if '=' in text:
        key, value = text.strip().split('=', 1)
        return key, yaml.unsafe_load(value)
    else:
        return text, True


def pytest_addoption(parser):
    import ssl
    parser.addoption(
        '--blocking-detection',
        action='store_true',
        dest='blocking_detection',
        default=False,
        help='turn on eventlet hub blocking detection')

    parser.addoption(
        "--amqp-uri", "--rabbit-amqp-uri",
        action="store",
        dest='RABBIT_AMQP_URI',
        default='pyamqp://guest:guest@localhost:5672/',
        help=(
            "URI for the RabbitMQ broker. Any specified virtual host will be "
            "ignored because tests run in their own isolated vhost."
        ))

    parser.addoption(
        "--rabbit-api-uri", "--rabbit-ctl-uri",
        action="store",
        dest='RABBIT_API_URI',
        default='http://guest:guest@localhost:15672',
        help=("URI for RabbitMQ management interface.")
    )

    parser.addoption(
        '--amqp-ssl-port',
        action='store',
        dest='AMQP_SSL_PORT',
        default=5671,
        help='Port number for SSL connection')

    parser.addoption(
        '--amqp-ssl-option',
        type=parse_config_option,
        action='append',
        dest='AMQP_SSL_OPTIONS',
        metavar='KEY=VALUE',
        default=[
            ('ca_certs', 'certs/cacert.pem'),
            ('certfile', 'certs/clientcert.pem'),
            ('keyfile', 'certs/clientkey.pem'),
            ('cert_reqs', ssl.CERT_REQUIRED)
        ],
        help=(
            'SSL connection options for passing to ssl.wrap_socket.'
            'Multiple options may be given. Values are parsed as YAML, '
            'hence the following example is valid: \n'
            '--amqp-ssl-option certfile=clientcert.pem '
            '--amqp-ssl-option ssl_version=!!python/name:ssl.PROTOCOL_TLSv1_2'
        )
    )


def pytest_load_initial_conftests():
    # make sure we monkey_patch before local conftests
    import eventlet
    eventlet.monkey_patch()


def pytest_configure(config):
    if config.option.blocking_detection:  # pragma: no cover
        from eventlet import debug
        debug.hub_blocking_detection(True)


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


@pytest.yield_fixture(scope='session')
def vhost_pipeline(request, rabbit_manager):
    from collections import Iterable
    from six.moves.urllib.parse import urlparse  # pylint: disable=E0401
    import random
    import socket
    import string
    from kombu.pools import connections
    from nameko.testing.utils import ResourcePipeline
    from nameko.utils.retry import retry
    from requests.exceptions import HTTPError

    rabbit_amqp_uri = request.config.getoption('RABBIT_AMQP_URI')
    uri_parts = urlparse(rabbit_amqp_uri)
    username = uri_parts.username

    def create():
        vhost = "nameko_test_{}".format(
            "".join(random.choice(string.ascii_lowercase) for _ in range(10))
        )
        rabbit_manager.create_vhost(vhost)
        rabbit_manager.set_vhost_permissions(
            vhost, username, '.*', '.*', '.*'
        )
        return vhost

    @retry(for_exceptions=(HTTPError, socket.timeout), delay=1, max_attempts=9)
    def destroy(vhost):
        rabbit_manager.delete_vhost(vhost)

        # make sure connections for this vhost are also destroyed.
        vhost_pools = [
            pool for key, pool in list(connections.items())
            if isinstance(key, Iterable) and key[4] == vhost
        ]
        for pool in vhost_pools:
            pool.force_close_all()

    pipeline = ResourcePipeline(create, destroy)

    with pipeline.run() as vhosts:
        yield vhosts


@pytest.yield_fixture()
def rabbit_config(request, vhost_pipeline, rabbit_manager):
    from six.moves.urllib.parse import urlparse  # pylint: disable=E0401

    rabbit_amqp_uri = request.config.getoption('RABBIT_AMQP_URI')
    uri_parts = urlparse(rabbit_amqp_uri)
    username = uri_parts.username

    with vhost_pipeline.get() as vhost:

        amqp_uri = "{uri.scheme}://{uri.netloc}/{vhost}".format(
            uri=uri_parts, vhost=vhost
        )

        conf = {
            'AMQP_URI': amqp_uri,
            'username': username,
            'vhost': vhost
        }

        yield conf


@pytest.fixture()
def rabbit_ssl_config(request, rabbit_config):
    from six.moves.urllib.parse import urlparse  # pylint: disable=E0401

    ssl_options = request.config.getoption('AMQP_SSL_OPTIONS')
    ssl_options = {key: value for key, value in ssl_options} or True

    amqp_ssl_port = request.config.getoption('AMQP_SSL_PORT')
    uri_parts = urlparse(rabbit_config['AMQP_URI'])
    amqp_ssl_uri = uri_parts._replace(
        netloc=uri_parts.netloc.replace(
            str(uri_parts.port),
            str(amqp_ssl_port))
    ).geturl()

    conf = {
        'AMQP_URI': amqp_ssl_uri,
        'username': rabbit_config['username'],
        'vhost': rabbit_config['vhost'],
        'AMQP_SSL': ssl_options,
    }

    return conf


@pytest.fixture
def amqp_uri(rabbit_config):
    from nameko.constants import AMQP_URI_CONFIG_KEY

    return rabbit_config[AMQP_URI_CONFIG_KEY]


@pytest.yield_fixture(autouse=True)
def fast_teardown(request):
    """
    This fixture fixes the order of the `container_factory`, `runner_factory`
    and `rabbit_config` fixtures to get the fastest possible teardown of tests
    that use them.

    Without this fixture, the teardown order depends on the fixture resolution
    defined by the test, for example::

    def test_foo(container_factory, rabbit_config):
        pass  # rabbit_config tears down first

    def test_bar(rabbit_config, container_factory):
        pass  # container_factory tears down first

    This fixture ensures the teardown order is:

        1. `fast_teardown`  (this fixture)
        2. `rabbit_config`
        3. `container_factory` / `runner_factory`

    That is, `rabbit_config` teardown, which removes the vhost created for
    the test, happens *before* the consumers are stopped.

    Deleting the vhost causes the broker to sends a "basic-cancel" message
    to any connected consumers, which will include the consumers in all
    containers created by the `container_factory` and `runner_factory`
    fixtures.

    This speeds up test teardown because the "basic-cancel" breaks
    the consumers' `drain_events` loop (http://bit.do/kombu-drain-events)
    which would otherwise wait for up to a second for the socket read to time
    out before gracefully shutting down.

    For even faster teardown, we monkeypatch the consumers to ensure they
    don't try to reconnect between the "basic-cancel" and being explicitly
    stopped when their container is killed.

    In older versions of RabbitMQ, the monkeypatch also protects against a
    race-condition that can lead to hanging tests.

    Modern RabbitMQ raises a `NotAllowed` exception if you try to connect to
    a vhost that doesn't exist, but older versions (including 3.4.3, used by
    Travis) just raise a `socket.error`. This is classed as a recoverable
    error, and consumers attempt to reconnect. Kombu's reconnection code blocks
    until a connection is established, so consumers that attempt to reconnect
    before being killed get stuck there.
    """
    from kombu.mixins import ConsumerMixin

    reorder_fixtures = ('container_factory', 'runner_factory', 'rabbit_config')
    for fixture in reorder_fixtures:
        if fixture in request.fixturenames:
            request.getfixturevalue(fixture)  # pragma: no cover

    consumers = []

    # monkeypatch the ConsumerMixin constructor to stash a reference to
    # each instance
    orig_init = ConsumerMixin.__init__

    def __init__(self, *args, **kwargs):
        orig_init(self, *args, **kwargs)
        consumers.append(self)

    ConsumerMixin.__init__ = __init__

    yield

    ConsumerMixin.__init__ = orig_init

    # set the `should_stop` attribute on all consumers *before* the rabbit
    # vhost is killed, so that they don't try to reconnect before they're
    # explicitly killed when their container stops.
    for consumer in consumers:
        consumer.should_stop = True


@pytest.fixture
def get_message_from_queue(amqp_uri):
    from nameko.amqp import get_connection

    def get(queue_name, ack=True, block=True, timeout=1, accept=None):
        with get_connection(amqp_uri) as conn:
            queue = conn.SimpleQueue(queue_name)

            # queue doesn't allow passing of `accept` to its consumer,
            # so we patch it on after instantiation
            if accept is not None:
                queue.consumer.accept = accept

            message = queue.get(block=block, timeout=timeout)
            if ack:
                message.ack()
            else:
                message.requeue()
            queue.close()
        return message

    return get


@pytest.yield_fixture
def container_factory():
    from nameko.containers import get_container_cls

    all_containers = []

    def make_container(service_cls, config):

        container_cls = get_container_cls(config)
        container = container_cls(service_cls, config)
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
    cfg[WEB_SERVER_CONFIG_KEY] = "127.0.0.1:{}".format(port)
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
