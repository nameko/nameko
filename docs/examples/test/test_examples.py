""" Tests for the files and snippets in nameko/docs/examples
"""
import os

from mock import call, patch
import requests

from nameko.standalone.events import event_dispatcher
from nameko.standalone.rpc import ServiceRpcProxy, ClusterRpcProxy
from nameko.testing.services import entrypoint_waiter


class TestHttp(object):

    base_url = "http://localhost:8000"

    def test_http(self, container_factory, rabbit_config):

        from examples.http import HttpService

        container = container_factory(HttpService, rabbit_config)
        container.start()

        res = requests.get(self.base_url + "/get/42")
        assert res.status_code == 200
        assert res.text == '{"value": 42}'

        res = requests.post(self.base_url + "/post", data="Hello")
        assert res.status_code == 200
        assert res.text == 'received: Hello'

    def test_advanced(self, container_factory, rabbit_config):

        from examples.advanced_http import Service

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

        from examples.events import ServiceA, ServiceB

        container_a = container_factory(ServiceA, rabbit_config)
        container_b = container_factory(ServiceB, rabbit_config)
        container_a.start()
        container_b.start()

        with ServiceRpcProxy('service_a', rabbit_config) as service_a_rpc:

            with patch.object(ServiceB, 'handle_event') as handle_event:

                with entrypoint_waiter(container_b, 'handle_event'):
                    service_a_rpc.dispatching_method("event payload")
                assert handle_event.call_args_list == [call("event payload")]

            # test without the patch to catch any errors in the handler method
            with entrypoint_waiter(container_b, 'handle_event'):
                service_a_rpc.dispatching_method("event payload")

    def test_standalone_events(self, container_factory, rabbit_config):

        from examples.events import ServiceB

        container_b = container_factory(ServiceB, rabbit_config)
        container_b.start()

        # standalone example doesn't import due to undefined variables
        # use execfile with a local namespace
        ns = {'AMQP_URI': rabbit_config['AMQP_URI']}

        dirpath = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        filepath = os.path.join(dirpath, 'standalone_events.py')

        with entrypoint_waiter(container_b, 'handle_event'):
            with open(filepath) as f:
                code = compile(f.read(), filepath, 'exec')
                exec(code, globals(), ns)

    def test_event_broadcast(self, container_factory, rabbit_config):

        from examples.event_broadcast import ListenerService

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

        # test without the patch to catch any errors in the handler method
        with entrypoint_waiter(container_1, 'ping'):
            dispatch("monitor", "ping", "payload")


class TestAnatomy(object):

    def test_anatomy(self, container_factory, rabbit_config):

        from examples.anatomy import Service

        container = container_factory(Service, rabbit_config)
        container.start()

        with ServiceRpcProxy('service', rabbit_config) as service_rpc:
            assert service_rpc.method() is None


class TestHelloWorld(object):

    def test_hello_world(self, container_factory, rabbit_config):

        from examples.helloworld import GreetingService

        container = container_factory(GreetingService, rabbit_config)
        container.start()

        with ServiceRpcProxy('greeting_service', rabbit_config) as greet_rpc:
            assert greet_rpc.hello("Matt") == "Hello, Matt!"


class TestRpc(object):

    def test_rpc(self, container_factory, rabbit_config):

        from examples.rpc import ServiceX, ServiceY

        container_x = container_factory(ServiceX, rabbit_config)
        container_y = container_factory(ServiceY, rabbit_config)
        container_x.start()
        container_y.start()

        with ServiceRpcProxy('service_x', rabbit_config) as service_x_rpc:
            assert service_x_rpc.remote_method("foo") == "foo-x-y"

    def test_standalone_rpc(self, container_factory, rabbit_config):

        from examples.rpc import ServiceX, ServiceY

        container_x = container_factory(ServiceX, rabbit_config)
        container_y = container_factory(ServiceY, rabbit_config)
        container_x.start()
        container_y.start()

        # standalone example doesn't import due to undefined variables
        # use execfile with a local namespace
        ns = {'AMQP_URI': rabbit_config['AMQP_URI']}

        dirpath = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        filepath = os.path.join(dirpath, 'standalone_rpc.py')

        with entrypoint_waiter(container_x, 'remote_method'):
            with open(filepath) as f:
                code = compile(f.read(), filepath, 'exec')
                exec(code, globals(), ns)

    def test_async_rpc(self, container_factory, rabbit_config):

        from examples.rpc import ServiceX, ServiceY

        container_x = container_factory(ServiceX, rabbit_config)
        container_y = container_factory(ServiceY, rabbit_config)
        container_x.start()
        container_y.start()

        # async example doesn't import due to undefined variables
        # use execfile with a local namespace
        ns = {
            'config': rabbit_config,
            'ClusterRpcProxy': ClusterRpcProxy
        }

        dirpath = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        filepath = os.path.join(dirpath, 'async_rpc.py')

        with entrypoint_waiter(container_x, 'remote_method'):
            with open(filepath) as f:
                code = compile(f.read(), filepath, 'exec')
                exec(code, globals(), ns)


class TestServiceContainer(object):

    def test_service_container(self):
        from examples import service_container


class TestServiceRunner(object):

    def test_service_runner(self):
        from examples import service_runner


class TestTimer(object):

    def test_timer(self, container_factory, rabbit_config):

        from examples.timer import Service

        container = container_factory(Service, rabbit_config)

        with entrypoint_waiter(container, 'ping'):
            container.start()


class TestTravis(object):

    def test_travis(self, container_factory, rabbit_config):

        from examples.travis import Travis

        container = container_factory(Travis, rabbit_config)
        container.start()

        with ServiceRpcProxy('travis_service', rabbit_config) as travis_rpc:
            status = travis_rpc.status_message("travis-ci", "cpython-builder")
            assert "Project travis-ci/cpython-builder" in status


class TestWebsocketRpc(object):

    def test_websocket_rpc(self, container_factory, web_config, websocket):

        from examples.websocket_rpc import WebsocketRpc

        container = container_factory(WebsocketRpc, web_config)
        container.start()

        ws = websocket()
        assert ws.rpc('echo', value="hello") == 'hello'
