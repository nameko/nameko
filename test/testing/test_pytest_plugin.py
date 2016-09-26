import socket

import pytest

from nameko.constants import AMQP_URI_CONFIG_KEY, WEB_SERVER_CONFIG_KEY
from nameko.extensions import DependencyProvider
from nameko.rpc import RpcProxy, rpc
from nameko.standalone.rpc import ServiceRpcProxy
from nameko.testing import rabbit
from nameko.testing.utils import get_rabbit_connections
from nameko.web.handlers import http
from nameko.web.server import parse_address
from nameko.web.websocket import rpc as wsrpc

pytest_plugins = "pytester"


def test_empty_config(empty_config):
    assert AMQP_URI_CONFIG_KEY in empty_config


def test_rabbit_manager(rabbit_manager):
    assert isinstance(rabbit_manager, rabbit.Client)
    assert "/" in [vhost['name'] for vhost in rabbit_manager.get_all_vhosts()]


def test_rabbit_config_random_vhost(testdir):

    testdir.makepyfile(
        """
        import re

        def test_rabbit_config(rabbit_config):
            assert re.search("test_[a-z]+$", rabbit_config['AMQP_URI'])
        """
    )
    result = testdir.runpytest()
    assert result.ret == 0


def test_rabbit_config_specific_vhost(testdir):

    testdir.makepyfile(
        """
        def test_rabbit_config(rabbit_config):
            assert "specified_vhost" in rabbit_config['AMQP_URI']
        """
    )
    result = testdir.runpytest(
        "--amqp-uri", "amqp://guest:guest@localhost:5672/specified_vhost"
    )
    assert result.ret == 0


def test_rabbit_config_leftover_connections(testdir):

    # run a test that leaves connections lying around
    testdir.makepyfile(
        """
        from nameko.containers import ServiceContainer
        from nameko.rpc import rpc

        class Service(object):
            name = "service"

            @rpc
            def method(self):
                pass

        def test_rabbit_config(rabbit_config):

            # not using container factory; will leave connections behind
            container = ServiceContainer(Service, rabbit_config)
            container.start()
        """
    )

    result = testdir.runpytest()
    assert result.ret == 1
    result.stdout.fnmatch_lines(
        ["*RuntimeError: 1 rabbit connection(s) left open*"]
    )


def test_cleanup_order(testdir):

    # without ``ensure_cleanup_order``, the following fixture ordering would
    # tear down ``rabbit_config`` before the ``container_factory`` (generating
    # an error about rabbit connections being left open)
    testdir.makepyfile(
        """
        from nameko.rpc import rpc

        class Service(object):
            name = "service"

            @rpc
            def method(self):
                pass

        def test_service(container_factory, rabbit_config):
            container = container_factory(Service, rabbit_config)
            container.start()
        """
    )
    result = testdir.runpytest()
    assert result.ret == 0


def test_container_factory(testdir, rabbit_config, rabbit_manager):

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
    result = testdir.runpytest()
    assert result.ret == 0

    vhost = rabbit_config['vhost']
    assert get_rabbit_connections(vhost, rabbit_manager) == []


def test_container_factory_with_custom_container_cls(testdir):

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
    result = testdir.runpytest()
    assert result.ret == 0


def test_container_factory_custom_worker_ctx_deprecation_warning(testdir):

    testdir.makeconftest(
        """
        from mock import patch
        import pytest

        @pytest.yield_fixture
        def warnings():
            with patch('nameko.containers.warnings') as patched:
                yield patched
        """
    )

    testdir.makepyfile(
        """
        from mock import ANY, call

        from nameko.containers import WorkerContext
        from nameko.rpc import rpc
        from nameko.standalone.rpc import ServiceRpcProxy

        class ServiceX(object):
            name = "x"

            @rpc
            def method(self):
                return "OK"

        def test_container_factory(
            container_factory, rabbit_config, warnings
        ):
            class WorkerContextX(WorkerContext):
                pass

            container = container_factory(
                ServiceX, rabbit_config, worker_ctx_cls=WorkerContextX
            )
            container.start()

            # TODO: replace with pytest.warns when eventlet >= 0.19.0 is
            # released
            assert warnings.warn.call_args_list == [
                call(ANY, DeprecationWarning)
            ]
        """
    )
    result = testdir.runpytest()
    assert result.ret == 0


def test_runner_factory(testdir, rabbit_config, rabbit_manager):

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
    result = testdir.runpytest()
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
