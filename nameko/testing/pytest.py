from __future__ import absolute_import

import pytest
import six

from nameko import config


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


@pytest.yield_fixture
def empty_config():
    with config.patch({}, clear=True):
        yield


@pytest.yield_fixture
def mock_container(request):
    from mock import create_autospec
    from nameko.constants import SERIALIZER_CONFIG_KEY, DEFAULT_SERIALIZER
    from nameko.containers import ServiceContainer

    container = create_autospec(ServiceContainer)

    with config.patch({SERIALIZER_CONFIG_KEY: DEFAULT_SERIALIZER}):
        container.serializer = config[SERIALIZER_CONFIG_KEY]
        container.accept = [DEFAULT_SERIALIZER]
        yield container


@pytest.fixture(scope='session')
def rabbit_manager(request):
    from nameko.testing import rabbit

    config = request.config
    return rabbit.Client(config.getoption('RABBIT_API_URI'))


@pytest.yield_fixture(scope='session')
def vhost_pipeline(request, rabbit_manager):
    if six.PY2:  # pragma: no cover
        from collections import Iterable  # pylint: disable=E0611
    else:  # pragma: no cover
        from collections.abc import Iterable  # pylint: disable=E0611,E0401
    from six.moves.urllib.parse import urlparse  # pylint: disable=E0401
    import random
    import string
    from kombu.pools import connections
    from nameko.testing.utils import ResourcePipeline
    from nameko.utils.retry import retry

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

    @retry(for_exceptions=Exception, delay=1, max_attempts=9)
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


@pytest.yield_fixture
def rabbit_uri(request, vhost_pipeline):
    from six.moves.urllib.parse import urlparse  # pylint: disable=E0401

    rabbit_amqp_uri = request.config.getoption('RABBIT_AMQP_URI')
    uri_parts = urlparse(rabbit_amqp_uri)

    with vhost_pipeline.get() as vhost:

        amqp_uri = "{uri.scheme}://{uri.netloc}/{vhost}".format(
            uri=uri_parts, vhost=vhost
        )

        yield amqp_uri


@pytest.yield_fixture
def rabbit_config(rabbit_uri):
    with config.patch({'AMQP_URI': rabbit_uri}):
        yield


@pytest.fixture
def rabbit_ssl_options(request):
    import os
    from test import on_travis

    ssl_options = request.config.getoption('AMQP_SSL_OPTIONS')
    ssl_options = {key: value for key, value in ssl_options} or True

    for file_key in ['ca_certs', 'certfile', 'keyfile']:
        filename = ssl_options.get(file_key)
        if (
                not on_travis and
                filename and not os.path.isfile(filename)
        ):  # pragma: no cover
            pytest.skip('ssl {} file {} does not exist'
                        .format(file_key, filename))

    return ssl_options


@pytest.fixture
def rabbit_ssl_uri(request, rabbit_uri):
    from six.moves.urllib.parse import urlparse  # pylint: disable=E0401

    amqp_ssl_port = request.config.getoption('AMQP_SSL_PORT')
    uri_parts = urlparse(rabbit_uri)
    amqp_ssl_uri = uri_parts._replace(
        netloc=uri_parts.netloc.replace(
            str(uri_parts.port),
            str(amqp_ssl_port))
    ).geturl()

    return amqp_ssl_uri


@pytest.yield_fixture()
def rabbit_ssl_config(request, rabbit_ssl_uri, rabbit_ssl_options):

    conf = {
        'AMQP_URI': rabbit_ssl_uri,
        'AMQP_SSL': rabbit_ssl_options,
    }

    with config.patch(conf):
        yield


@pytest.fixture
def amqp_uri(rabbit_config):
    from nameko.constants import AMQP_URI_CONFIG_KEY

    return config[AMQP_URI_CONFIG_KEY]


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
    import nameko
    from nameko.containers import get_container_cls

    all_containers = []

    def make_container(service_cls, config=None):

        # nameko 2.X backward compatible passing of custom config argument
        # we apply config patch if a config dictionary is passed to the factory
        if config:
            patch = nameko.config.patch(config)
            patch.start()
        else:
            patch = None

        container_cls = get_container_cls()
        container = container_cls(service_cls)
        all_containers.append((container, patch))
        return container

    yield make_container

    for container, patch in reversed(all_containers):
        try:
            container.kill()
        except:  # pragma: no cover
            pass
        if patch:
            patch.stop()


@pytest.yield_fixture
def runner_factory():
    import collections
    import nameko
    from nameko.runners import ServiceRunner

    all_runners = []

    def make_runner(*service_classes):

        # nameko 2.X backward compatible passing of custom config argument
        # we apply config patch if a config dictionary is passed to the factory
        if service_classes and isinstance(service_classes[0], collections.Mapping):
            config = service_classes[0]
            service_classes = service_classes[1:]
            patch = nameko.config.patch(config)
            patch.start()
        else:
            patch = None

        runner = ServiceRunner()
        for service_cls in service_classes:
            runner.add_service(service_cls)
        all_runners.append((runner, patch))
        return runner

    yield make_runner

    for runner, patch in reversed(all_runners):
        try:
            runner.kill()
        except:  # pragma: no cover
            pass
        if patch:
            patch.stop()


@pytest.yield_fixture
def predictable_call_ids(request):
    import itertools
    from mock import patch

    with patch('nameko.standalone.rpc.uuid') as client_uuid:
        client_uuid.uuid4.side_effect = (str(i) for i in itertools.count())
        with patch('nameko.containers.uuid', autospec=True) as call_uuid:
            call_uuid.uuid4.side_effect = (str(i) for i in itertools.count())
            yield call_uuid.uuid4


@pytest.yield_fixture
def web_config():
    from nameko.constants import WEB_SERVER_CONFIG_KEY
    from nameko.testing.utils import find_free_port

    port = find_free_port()

    with config.patch({WEB_SERVER_CONFIG_KEY: "127.0.0.1:{}".format(port)}):
        yield


@pytest.fixture()
def web_config_port(web_config):
    from nameko.constants import WEB_SERVER_CONFIG_KEY
    from nameko.web.server import parse_address
    return parse_address(config[WEB_SERVER_CONFIG_KEY]).port


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
