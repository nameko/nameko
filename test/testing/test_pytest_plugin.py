import socket
import time

import pytest
from six.moves import queue

from nameko.constants import WEB_SERVER_CONFIG_KEY
from nameko.extensions import DependencyProvider
from nameko.rpc import RpcProxy, rpc
from nameko.standalone.rpc import ServiceRpcProxy
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

            def test_ssl_options(request, rabbit_ssl_config):
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
                assert rabbit_ssl_config['AMQP_SSL'] == expected_ssl_options
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
    assert empty_config == {}


def test_rabbit_manager(rabbit_manager):
    assert isinstance(rabbit_manager, rabbit.Client)
    assert "/" in [vhost['name'] for vhost in rabbit_manager.get_all_vhosts()]


def test_amqp_uri(testdir):

    amqp_uri = "amqp://user:pass@host:5672/vhost"

    testdir.makeconftest(
        """
        import pytest

        @pytest.fixture
        def rabbit_config():
            return dict(AMQP_URI='{}')
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
    def publish_message(self, rabbit_manager, rabbit_config, queue_name):
        vhost = rabbit_config['vhost']
        rabbit_manager.create_queue(vhost, queue_name, durable=True)

        def publish(payload, **properties):
            rabbit_manager.publish(
                vhost, "amq.default", queue_name, payload, properties
            )

        return publish

    def test_get_message(
        self, publish_message, get_message_from_queue, queue_name,
        rabbit_manager, rabbit_config
    ):
        payload = "payload"
        publish_message(payload)

        message = get_message_from_queue(queue_name)
        assert message.payload == payload

        vhost = rabbit_config['vhost']
        assert rabbit_manager.get_queue(vhost, queue_name)['messages'] == 0

    def test_requeue(
        self, publish_message, get_message_from_queue, queue_name,
        rabbit_manager, rabbit_config
    ):
        payload = "payload"
        publish_message(payload)

        message = get_message_from_queue(queue_name, ack=False)
        assert message.payload == payload

        time.sleep(1)  # TODO: use retry decorator rather than sleep
        vhost = rabbit_config['vhost']
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


class TestFastTeardown(object):

    def test_order(self, testdir):

        testdir.makeconftest(
            """
            from mock import Mock
            import pytest

            @pytest.fixture(scope='session')
            def tracker():
                return Mock()

            @pytest.yield_fixture
            def rabbit_config(tracker):
                tracker("rabbit_config", "up")
                yield
                tracker("rabbit_config", "down")

            @pytest.yield_fixture
            def container_factory(tracker):
                tracker("container_factory", "up")
                yield
                tracker("container_factory", "down")
            """
        )

        testdir.makepyfile(
            """
            from mock import call

            def test_foo(container_factory, rabbit_config):
                pass  # factory first

            def test_bar(rabbit_config, container_factory):
                pass  # rabbit first

            def test_check(tracker):
                assert tracker.call_args_list == [
                    # test_foo
                    call("container_factory", "up"),
                    call("rabbit_config", "up"),
                    call("rabbit_config", "down"),
                    call("container_factory", "down"),
                    # test_bar
                    call("container_factory", "up"),
                    call("rabbit_config", "up"),
                    call("rabbit_config", "down"),
                    call("container_factory", "down"),
                ]
            """
        )
        result = testdir.runpytest()
        assert result.ret == 0

    def test_only_affects_used_fixtures(self, testdir):

        testdir.makeconftest(
            """
            from mock import Mock
            import pytest

            @pytest.fixture(scope='session')
            def tracker():
                return Mock()

            @pytest.yield_fixture
            def rabbit_config(tracker):
                tracker("rabbit_config", "up")
                yield
                tracker("rabbit_config", "down")

            @pytest.yield_fixture
            def container_factory(tracker):
                tracker("container_factory", "up")
                yield
                tracker("container_factory", "down")
            """
        )

        testdir.makepyfile(
            """
            from mock import call

            def test_no_rabbit(container_factory):
                pass  # factory first

            def test_check(tracker):
                assert tracker.call_args_list == [
                    call("container_factory", "up"),
                    call("container_factory", "down"),
                ]
            """
        )
        result = testdir.runpytest()
        assert result.ret == 0

    def test_consumer_mixin_patch(self, testdir):

        testdir.makeconftest(
            """
            from kombu.mixins import ConsumerMixin
            import pytest

            consumers = []

            @pytest.fixture(autouse=True)
            def fast_teardown(patch_checker, fast_teardown):
                ''' Shadow the fast_teardown fixture to set fixture order:

                Setup:

                    1. patch_checker
                    2. original fast_teardown (applies monkeypatch)
                    3. this fixture (creates consumer)

                Teardown:

                    1. this fixture (creates consumer)
                    2. original fast_teardown (removes patch; sets attribute)
                    3. patch_checker (verifies consumer was stopped)
                '''
                consumers.append(ConsumerMixin())

            @pytest.yield_fixture
            def patch_checker():
                yield
                assert consumers[0].should_stop is True
            """
        )

        testdir.makepyfile(
            """
            def test_mixin_patch(patch_checker):
                pass
            """
        )
        result = testdir.runpytest()
        assert result.ret == 0


def test_container_factory(
    testdir, rabbit_config, rabbit_manager, plugin_options
):

    testdir.makepyfile(
        """
        from nameko.rpc import rpc
        from nameko.standalone.rpc import ServiceRpcProxy

        class ServiceX(object):
            name = "x"

            @rpc
            def method(self):
                return "OK"

        def test_container_factory(container_factory, rabbit_config):
            container = container_factory(ServiceX, rabbit_config)
            container.start()

            with ServiceRpcProxy("x", rabbit_config) as proxy:
                assert proxy.method() == "OK"
        """
    )
    result = testdir.runpytest(*plugin_options)
    assert result.ret == 0

    vhost = rabbit_config['vhost']
    assert get_rabbit_connections(vhost, rabbit_manager) == []


def test_container_factory_with_custom_container_cls(testdir, plugin_options):

    testdir.makepyfile(container_module="""
        from nameko.containers import ServiceContainer

        class ServiceContainerX(ServiceContainer):
            pass
    """)

    testdir.makepyfile(
        """
        from nameko.rpc import rpc
        from nameko.standalone.rpc import ServiceRpcProxy

        from container_module import ServiceContainerX

        class ServiceX(object):
            name = "x"

            @rpc
            def method(self):
                return "OK"

        def test_container_factory(
            container_factory, rabbit_config
        ):
            rabbit_config['SERVICE_CONTAINER_CLS'] = (
                "container_module.ServiceContainerX"
            )

            container = container_factory(ServiceX, rabbit_config)
            container.start()

            assert isinstance(container, ServiceContainerX)

            with ServiceRpcProxy("x", rabbit_config) as proxy:
                assert proxy.method() == "OK"
        """
    )
    result = testdir.runpytest(*plugin_options)
    assert result.ret == 0


def test_runner_factory(
    testdir, plugin_options, rabbit_config, rabbit_manager
):

    testdir.makepyfile(
        """
        from nameko.rpc import rpc
        from nameko.standalone.rpc import ServiceRpcProxy

        class ServiceX(object):
            name = "x"

            @rpc
            def method(self):
                return "OK"

        def test_runner(runner_factory, rabbit_config):
            runner = runner_factory(rabbit_config, ServiceX)
            runner.start()

            with ServiceRpcProxy("x", rabbit_config) as proxy:
                assert proxy.method() == "OK"
        """
    )
    result = testdir.runpytest(*plugin_options)
    assert result.ret == 0

    vhost = rabbit_config['vhost']
    assert get_rabbit_connections(vhost, rabbit_manager) == []


@pytest.mark.usefixtures('predictable_call_ids')
def test_predictable_call_ids(runner_factory, rabbit_config):

    worker_contexts = []

    class CaptureWorkerContext(DependencyProvider):
        def worker_setup(self, worker_ctx):
            worker_contexts.append(worker_ctx)

    class ServiceX(object):
        name = "x"

        capture = CaptureWorkerContext()
        service_y = RpcProxy("y")

        @rpc
        def method(self):
            self.service_y.method()

    class ServiceY(object):
        name = "y"

        capture = CaptureWorkerContext()

        @rpc
        def method(self):
            pass

    runner = runner_factory(rabbit_config, ServiceX, ServiceY)
    runner.start()

    with ServiceRpcProxy("x", rabbit_config) as service_x:
        service_x.method()

    call_ids = [worker_ctx.call_id for worker_ctx in worker_contexts]
    assert call_ids == ["x.method.1", "y.method.2"]


def test_web_config(web_config):
    assert WEB_SERVER_CONFIG_KEY in web_config

    bind_address = parse_address(web_config[WEB_SERVER_CONFIG_KEY])
    sock = socket.socket()
    sock.bind(bind_address)


def test_web_session(web_config, container_factory, web_session):

    class Service(object):
        name = "web"

        @http('GET', '/foo')
        def method(self, request):
            return "OK"

    container = container_factory(Service, web_config)
    container.start()

    assert web_session.get("/foo").status_code == 200


def test_websocket(web_config, container_factory, websocket):

    class Service(object):
        name = "ws"

        @wsrpc
        def uppercase(self, socket_id, arg):
            return arg.upper()

    container = container_factory(Service, web_config)
    container.start()

    ws = websocket()
    assert ws.rpc("uppercase", arg="foo") == "FOO"
