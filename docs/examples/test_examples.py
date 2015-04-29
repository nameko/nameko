""" Tests for the files in nameko/docs/examples
"""
import os

import eventlet
from mock import call, patch
import pytest
import requests

from nameko.containers import ServiceContainer
from nameko.standalone.events import event_dispatcher
from nameko.standalone.rpc import ServiceRpcProxy
from nameko.testing.services import entrypoint_waiter
from nameko.testing.websocket import make_virtual_socket


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
        except:
            pass


@pytest.yield_fixture()
def websocket():
    active_sockets = []

    def socket_creator():
        ws_app, wait_for_sock = make_virtual_socket(
            '127.0.0.1', 8000)
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


class TestHttp(object):

    base_url = "http://localhost:8000"

    def test_http(self, container_factory, rabbit_config):

        from http import HttpService

        container = container_factory(HttpService, rabbit_config)
        container.start()

        res = requests.get(self.base_url + "/get/42")
        assert res.status_code == 200
        assert res.text == '{"value": 42}'

        res = requests.post(self.base_url + "/post", data="Hello")
        assert res.status_code == 200
        assert res.text == 'received: Hello'

    def test_advanced(self, container_factory, rabbit_config):

        from advanced_http import Service

        container = container_factory(Service, rabbit_config)
        container.start()

        res = requests.get(self.base_url + "/privileged")
        assert res.status_code == 403
        assert res.text == 'Forbidden'

        res = requests.get(self.base_url + "/headers")
        assert res.status_code == 201
        assert res.headers['location'] == 'https://www.example.com/widget/1'

        res = requests.get(self.base_url + "/custom")
        assert res.status_code == 200
        assert res.text == 'payload'


class TestEvents(object):

    def test_events(self, container_factory, rabbit_config):

        from events import ServiceA, ServiceB

        container_a = container_factory(ServiceA, rabbit_config)
        container_b = container_factory(ServiceB, rabbit_config)
        container_a.start()
        container_b.start()

        with ServiceRpcProxy('service_a', rabbit_config) as service_a_rpc:

            with patch.object(ServiceB, 'handle_event') as handle_event:

                with entrypoint_waiter(container_b, 'handle_event'):
                    service_a_rpc.dispatching_method()
                assert handle_event.call_args_list == [call("payload")]

    def test_event_broadcast(self, container_factory, rabbit_config):

        from event_broadcast import ListenerService

        container_1 = container_factory(ListenerService, rabbit_config)
        container_2 = container_factory(ListenerService, rabbit_config)
        container_1.start()
        container_2.start()

        dispatch = event_dispatcher(rabbit_config)

        with patch.object(ListenerService, 'ping') as ping:

            waiter_1 = entrypoint_waiter(container_1, 'ping')
            waiter_2 = entrypoint_waiter(container_2, 'ping')

            with waiter_1, waiter_2:
                dispatch("monitor", "ping", "payload")
            assert ping.call_count == 2


class TestExample(object):

    def test_example(self, container_factory, rabbit_config):

        from example import Service

        container = container_factory(Service, rabbit_config)
        container.start()

        with ServiceRpcProxy('service', rabbit_config) as service_rpc:
            assert service_rpc.method() == None


class TestFibonacci(object):

    def test_fibonaccci(self, container_factory, rabbit_config):

        from fibonacci import Fibonacci

        container = container_factory(Fibonacci, rabbit_config)
        container.start()

        with ServiceRpcProxy('fibonacci', rabbit_config) as fibonacci_rpc:
            for number in [1, 2, 3, 5, 8, 13]:
                assert fibonacci_rpc.next() == number


class TestHelloWorld(object):

    def test_hello_world(self, container_factory, rabbit_config):

        from helloworld import GreetingService

        container = container_factory(GreetingService, rabbit_config)
        container.start()

        with ServiceRpcProxy('greeting_service', rabbit_config) as greet_rpc:
            assert greet_rpc.hello("Matt") == "Hello, Matt!"


class TestRpc(object):

    def test_rpc(self, container_factory, rabbit_config):

        from rpc import ServiceX, ServiceY

        container_x = container_factory(ServiceX, rabbit_config)
        container_y = container_factory(ServiceY, rabbit_config)
        container_x.start()
        container_y.start()

        with ServiceRpcProxy('service_x', rabbit_config) as service_x_rpc:
            assert service_x_rpc.remote_method("foo") == "foo-x-y"

    def test_standalone_rpc(self, container_factory, rabbit_config):

        from rpc import ServiceX, ServiceY

        container_x = container_factory(ServiceX, rabbit_config)
        container_y = container_factory(ServiceY, rabbit_config)
        container_x.start()
        container_y.start()

        # standalone example doesn't import due to undefined variables
        # use execfile with a local namespace
        ns = {'AMQP_URI': rabbit_config['AMQP_URI']}

        dirpath = os.path.dirname(os.path.realpath(__file__))
        filepath = os.path.join(dirpath, 'standalone_rpc.py')

        with entrypoint_waiter(container_x, 'remote_method'):
            execfile(filepath, globals(), ns)

    def test_async_rpc(self, container_factory, rabbit_config):

        from rpc import ServiceX, ServiceY

        container_x = container_factory(ServiceX, rabbit_config)
        container_y = container_factory(ServiceY, rabbit_config)
        container_x.start()
        container_y.start()

        # async example doesn't import due to undefined variables
        # use execfile with a local namespace
        from nameko.standalone.rpc import ClusterRpcProxy
        ns = {
            'config': rabbit_config,
            'ClusterRpcProxy': ClusterRpcProxy
        }

        dirpath = os.path.dirname(os.path.realpath(__file__))
        filepath = os.path.join(dirpath, 'async_rpc.py')

        with entrypoint_waiter(container_x, 'remote_method'):
            execfile(filepath, globals(), ns)


class TestServiceContainer(object):

    def test_service_container(self):
        from . import service_container


class TestServiceRunner(object):

    def test_service_runner(self):
        from . import service_runner


class TestTimer(object):

    def test_timer(self, container_factory, rabbit_config):

        from timer import Service

        container = container_factory(Service, rabbit_config)

        with entrypoint_waiter(container, 'ping'):
            container.start()


class TestWebsocketRpc(object):

    def test_websocket_rpc(self, container_factory, rabbit_config, websocket):

        from websocket_rpc import WebsocketRpc

        container = container_factory(WebsocketRpc, rabbit_config)
        container.start()

        ws = websocket()
        assert ws.rpc('echo', value="hello") == 'hello'
