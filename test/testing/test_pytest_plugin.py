import socket
import time

import pytest
from six.moves import queue

from nameko import config
from nameko.constants import WEB_SERVER_CONFIG_KEY
from nameko.extensions import DependencyProvider
from nameko.rpc import ServiceRpc, rpc
from nameko.standalone.rpc import ServiceRpcClient
from nameko.testing import rabbit
from nameko.testing.utils import get_rabbit_connections
from nameko.web.handlers import http
from nameko.web.server import parse_address
from nameko.web.websocket import rpc as wsrpc


pytest_plugins = "pytester"


@pytest.fixture
def plugin_options(request):
    """ Get the options pytest may have been invoked with so we can pass
    them into subprocess pytests created by the pytester plugin.
    """
    options = (
        '--rabbit-amqp-uri',
        '--rabbit-api-uri'
    )

    args = [
        "{}={}".format(opt, request.config.getoption(opt)) for opt in options
    ]
    return args


class TestOptions(object):

    def test_options(self, testdir):

        options = (
            ("--amqp-uri", 'amqp://localhost:5672/vhost'),
            ("--rabbit-api-uri", 'http://localhost:15672'),
            ("--amqp-ssl-port", '1234'),
        )

        testdir.makepyfile(
            """
            import re

            def test_option(request):
                assert request.config.getoption('RABBIT_AMQP_URI') == (
                    'amqp://localhost:5672/vhost'
                )
                assert request.config.getoption('RABBIT_API_URI') == (
                    'http://localhost:15672'
                )
                assert request.config.getoption('AMQP_SSL_PORT') == '1234'
            """
        )
        args = []
        for option, value in options:
            args.extend((option, value))
        result = testdir.runpytest(*args)
        assert result.ret == 0

    def test_ssl_options(self, testdir):

        options = (
            ('certfile', 'path/cert.pem'),  # override default
            ('string', 'string'),
            ('number', 1),
            ('list', '[1, 2, 3]'),
            ('map', '{"foo": "bar"}'),
            ('lookup', '!!python/name:ssl.CERT_REQUIRED'),
        )

        testdir.makepyfile(
            """
            import re
            import ssl

            import pytest

            from nameko import config

            @pytest.mark.usefixtures("rabbit_ssl_config")
            def test_ssl_options(request):
                assert request.config.getoption('AMQP_SSL_OPTIONS') == [
                    # defaults
                    ('ca_certs', 'certs/cacert.pem'),
                    ('certfile', 'certs/clientcert.pem'),
                    ('keyfile', 'certs/clientkey.pem'),
                    ('cert_reqs', ssl.CERT_REQUIRED),
                    # additions
                    ('certfile', 'path/cert.pem'),
                    ('string', 'string'),
                    ('number', 1),
                    ('list', [1, 2, 3]),
                    ('map', {'foo': 'bar'}),
                    ('lookup', ssl.CERT_REQUIRED),
                    ('keyonly', True),
                ]

                expected_ssl_options = {
                    'certfile': 'path/cert.pem',  # default overridden
                    'ca_certs': 'certs/cacert.pem',
                    'keyfile': 'certs/clientkey.pem',
                    'cert_reqs': ssl.CERT_REQUIRED,
                    'string': 'string',
                    'number': 1,
                    'list': [1, 2, 3],
                    'map': {'foo': 'bar'},
                    'lookup': ssl.CERT_REQUIRED,
                    'keyonly': True,
                }
                assert config['AMQP_SSL'] == expected_ssl_options
            """
        )
        args = []
        for key, value in options:
            args.extend(['--amqp-ssl-option', "{}={}".format(key, value)])
        # key only case
        args.extend(['--amqp-ssl-option', "keyonly"])

        result = testdir.runpytest(*args)
        assert result.ret == 0


def test_empty_config(empty_config):
    assert config == {}


def test_rabbit_manager(rabbit_manager):
    assert isinstance(rabbit_manager, rabbit.Client)
    assert "/" in [vhost['name'] for vhost in rabbit_manager.get_all_vhosts()]


def test_amqp_uri(testdir):

    amqp_uri = "amqp://user:pass@host:5672/vhost"

    testdir.makeconftest(
        """
        import pytest
        from nameko import config

        @pytest.yield_fixture
        def rabbit_config():
            with config.patch(dict(AMQP_URI="{}")):
                yield
        """.format(amqp_uri)
    )

    testdir.makepyfile(
        """
        import re

        def test_amqp_uri(amqp_uri):
            assert amqp_uri == '{}'
        """.format(amqp_uri)
    )
    result = testdir.runpytest(
        "--amqp-uri", amqp_uri
    )
    assert result.ret == 0


class TestGetMessageFromQueue(object):

    @pytest.fixture
    def queue_name(self):
        return "queue"

    @pytest.fixture
    def publish_message(
        self, rabbit_manager, rabbit_config, get_vhost, queue_name
    ):
        vhost = get_vhost(config['AMQP_URI'])
        rabbit_manager.create_queue(vhost, queue_name, durable=True)

        def publish(payload, **properties):
            rabbit_manager.publish(
                vhost, "amq.default", queue_name, payload, properties
            )

        return publish

    @pytest.mark.usefixtures("rabbit_config")
    def test_get_message(
        self, publish_message, get_message_from_queue, queue_name,
        rabbit_manager, get_vhost
    ):
        payload = "payload"
        publish_message(payload)

        message = get_message_from_queue(queue_name)
        assert message.payload == payload

        vhost = get_vhost(config['AMQP_URI'])
        assert rabbit_manager.get_queue(vhost, queue_name)['messages'] == 0

    @pytest.mark.usefixtures("rabbit_config")
    def test_requeue(
        self, publish_message, get_message_from_queue, queue_name,
        rabbit_manager, get_vhost
    ):
        payload = "payload"
        publish_message(payload)

        message = get_message_from_queue(queue_name, ack=False)
        assert message.payload == payload

        time.sleep(1)  # TODO: use retry decorator rather than sleep
        vhost = get_vhost(config['AMQP_URI'])
        assert rabbit_manager.get_queue(vhost, queue_name)['messages'] == 1

    def test_non_blocking(
        self, publish_message, get_message_from_queue, queue_name
    ):
        # no message published; raises immediately
        with pytest.raises(queue.Empty):
            get_message_from_queue(queue_name, block=False)

    def test_timeout(
        self, publish_message, get_message_from_queue, queue_name
    ):
        # no message published; raises after timeout
        with pytest.raises(queue.Empty):
            get_message_from_queue(queue_name, timeout=0.01)

    def test_accept(
        self, publish_message, get_message_from_queue, queue_name
    ):
        payload = "payload"
        content_type = "application/x-special"
        publish_message(payload, content_type=content_type)

        message = get_message_from_queue(queue_name, accept=content_type)
        assert message.properties['content_type'] == content_type
        assert message.payload == payload


@pytest.mark.usefixtures("rabbit_config")
def test_container_factory(
    testdir, get_vhost, rabbit_manager, plugin_options
):

    testdir.makepyfile(
        """
        import pytest

        from nameko.rpc import rpc
        from nameko.standalone.rpc import ServiceRpcClient

        class ServiceX(object):
            name = "x"

            @rpc
            def method(self):
                return "OK"

        @pytest.mark.usefixtures('rabbit_config')
        def test_container_factory(container_factory):
            container = container_factory(ServiceX)
            container.start()

            with ServiceRpcClient("x") as client:
                assert client.method() == "OK"
        """
    )
    result = testdir.runpytest(*plugin_options)
    assert result.ret == 0

    vhost = get_vhost(config['AMQP_URI'])
    assert get_rabbit_connections(vhost, rabbit_manager) == []


def test_container_factory_with_custom_container_cls(testdir, plugin_options):

    testdir.makepyfile(container_module="""
        from nameko.containers import ServiceContainer

        class ServiceContainerX(ServiceContainer):
            pass
    """)

    testdir.makepyfile(
        """
        import pytest

        from nameko import config
        from nameko.rpc import rpc
        from nameko.standalone.rpc import ServiceRpcClient

        from container_module import ServiceContainerX

        class ServiceX(object):
            name = "x"

            @rpc
            def method(self):
                return "OK"

        @pytest.mark.usefixtures('rabbit_config')
        def test_container_factory(container_factory):
            with config.patch({
                'SERVICE_CONTAINER_CLS': "container_module.ServiceContainerX"
            }):
                container = container_factory(ServiceX)
                container.start()

                assert isinstance(container, ServiceContainerX)

            with ServiceRpcClient("x") as client:
                assert client.method() == "OK"
        """
    )
    result = testdir.runpytest(*plugin_options)
    assert result.ret == 0


def test_container_factory_with_config_patch_argument(testdir):

    testdir.makepyfile(
        """
        import nameko
        import pytest

        from nameko.testing.services import dummy

        class ServiceX(object):
            name = "x"

            @dummy
            def method(self):
                return "OK"

        def test_container_factory_applies_config_patch_(container_factory):

            assert "FOO" not in nameko.config

            container = container_factory(ServiceX, {"FOO": 2})
            container.start()

            assert nameko.config["FOO"] == 2

        def test_container_factory_rolls_back_config_patch():

            assert "FOO" not in nameko.config
        """
    )
    result = testdir.runpytest()
    assert result.ret == 0


@pytest.mark.usefixtures("rabbit_config")
def test_runner_factory(
    testdir, plugin_options, get_vhost, rabbit_manager
):

    testdir.makepyfile(
        """
        import pytest

        from nameko.rpc import rpc
        from nameko.standalone.rpc import ServiceRpcClient

        class ServiceX(object):
            name = "x"

            @rpc
            def method(self):
                return "OK"

        @pytest.mark.usefixtures('rabbit_config')
        def test_runner(runner_factory):
            runner = runner_factory(ServiceX)
            runner.start()

            with ServiceRpcClient("x") as client:
                assert client.method() == "OK"
        """
    )
    result = testdir.runpytest(*plugin_options)
    assert result.ret == 0

    vhost = get_vhost(config['AMQP_URI'])
    assert get_rabbit_connections(vhost, rabbit_manager) == []


def test_runner_factory_with_config_patch_argument(testdir):

    testdir.makepyfile(
        """
        import nameko
        import pytest

        from nameko.testing.services import dummy

        class ServiceX(object):
            name = "x"

        class ServiceY(object):
            name = "y"

        def test_runner_factory_applies_config_patch_(runner_factory):

            assert "FOO" not in nameko.config

            runner = runner_factory({"FOO": 2}, ServiceX, ServiceY)
            runner.start()

            assert nameko.config["FOO"] == 2

        def test_runner_factory_rolls_back_config_patch():

            assert "FOO" not in nameko.config
        """
    )
    result = testdir.runpytest()
    assert result.ret == 0


@pytest.mark.usefixtures('rabbit_config')
@pytest.mark.usefixtures('predictable_call_ids')
def test_predictable_call_ids(runner_factory):

    worker_contexts = []

    class CaptureWorkerContext(DependencyProvider):
        def worker_setup(self, worker_ctx):
            worker_contexts.append(worker_ctx)

    class ServiceX(object):
        name = "x"

        capture = CaptureWorkerContext()
        service_y = ServiceRpc("y")

        @rpc
        def method(self):
            self.service_y.method()

    class ServiceY(object):
        name = "y"

        capture = CaptureWorkerContext()

        @rpc
        def method(self):
            pass

    runner = runner_factory(ServiceX, ServiceY)
    runner.start()

    with ServiceRpcClient("x") as service_x:
        service_x.method()

    call_ids = [worker_ctx.call_id for worker_ctx in worker_contexts]
    assert call_ids == ["x.method.1", "y.method.2"]


@pytest.mark.usefixtures('web_config')
def test_web_config():
    assert WEB_SERVER_CONFIG_KEY in config

    bind_address = parse_address(config[WEB_SERVER_CONFIG_KEY])
    sock = socket.socket()
    sock.bind(bind_address)


@pytest.mark.usefixtures('web_config')
def test_web_session(container_factory, web_session):

    class Service(object):
        name = "web"

        @http('GET', '/foo')
        def method(self, request):
            return "OK"

    container = container_factory(Service)
    container.start()

    assert web_session.get("/foo").status_code == 200


@pytest.mark.usefixtures('web_config')
def test_websocket(container_factory, websocket):

    class Service(object):
        name = "ws"

        @wsrpc
        def uppercase(self, socket_id, arg):
            return arg.upper()

    container = container_factory(Service)
    container.start()

    ws = websocket()
    assert ws.rpc("uppercase", arg="foo") == "FOO"
