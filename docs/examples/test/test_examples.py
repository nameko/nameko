# coding: utf-8
""" Tests for the files and snippets in nameko/docs/examples
"""
import json
import os

from mock import call, patch, Mock
from moto import mock_sqs
import boto3
import jwt
import pytest

from nameko.exceptions import RemoteError
from nameko.standalone.events import event_dispatcher
from nameko.standalone.rpc import ClusterRpcProxy, ServiceRpcProxy
from nameko.testing.services import dummy, entrypoint_waiter, entrypoint_hook
from nameko.web.handlers import http


class TestHttp(object):

    def test_http(self, container_factory, web_config, web_session):

        from examples.http import HttpService

        container = container_factory(HttpService, web_config)
        container.start()

        res = web_session.get("/get/42")
        assert res.status_code == 200
        assert res.text == '{"value": 42}'

        res = web_session.post("/post", data=u"你好".encode('utf-8'))
        assert res.status_code == 200
        assert res.text == u'received: 你好'

        res = web_session.get("/multi")
        assert res.status_code == 200
        assert res.text == 'GET'

        res = web_session.put("/multi")
        assert res.status_code == 200
        assert res.text == 'PUT'

        res = web_session.post("/multi")
        assert res.status_code == 200
        assert res.text == 'POST'

        res = web_session.delete("/multi")
        assert res.status_code == 200
        assert res.text == 'DELETE'

    def test_advanced(self, container_factory, web_config, web_session):

        from examples.advanced_http import Service

        container = container_factory(Service, web_config)
        container.start()

        res = web_session.get("/privileged")
        assert res.status_code == 403
        assert res.text == 'Forbidden'

        res = web_session.get("/headers")
        assert res.status_code == 201
        assert res.headers['location'] == 'https://www.example.com/widget/1'

        res = web_session.get("/custom")
        assert res.status_code == 200
        assert res.text == 'payload'

    def test_custom_exception(self, container_factory, web_config, web_session):

        from examples.http_exceptions import Service

        container = container_factory(Service, web_config)
        container.start()

        res = web_session.get("/custom_exception")
        assert res.status_code == 400
        assert res.headers['Content-Type'] == 'application/json'
        assert res.json() == {
            'error': 'INVALID_ARGUMENTS',
            'message': "Argument `foo` is required.",
        }

    def test_will_not_handle_unknown_exception(self, container_factory, web_config, web_session):

        from examples.http_exceptions import http

        class Service(object):
            name = "service"

            @http('GET', '/exception')
            def exception(self, request):
                raise ValueError("Argument `foo` is required.")

        container = container_factory(Service, web_config)
        container.start()

        res = web_session.get("/exception")
        assert res.status_code == 500


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
                dispatch("monitor", "ping", "payløad")
            assert ping.call_count == 2

        # test without the patch to catch any errors in the handler method
        with entrypoint_waiter(container_1, 'ping'):
            dispatch("monitor", "ping", "payløad")


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
            assert greet_rpc.hello(u"Møtt") == u"Hello, Møtt!"


class TestRpc(object):

    def test_rpc(self, container_factory, rabbit_config):

        from examples.rpc import ServiceX, ServiceY

        container_x = container_factory(ServiceX, rabbit_config)
        container_y = container_factory(ServiceY, rabbit_config)
        container_x.start()
        container_y.start()

        with ServiceRpcProxy('service_x', rabbit_config) as service_x_rpc:
            assert service_x_rpc.remote_method(u"føø") == u"føø-x-y"

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

    @pytest.fixture
    def fake_travis(self, container_factory, web_config, web_session):

        class FakeTravis:
            name = "travis"

            @http("GET", "/fake_status")
            def method(self, request):
                return json.dumps({
                    "last_build_result": "success",
                    "slug": "nameko/nameko",
                    "last_build_finished_at": "2020-01-01"
                })

        container = container_factory(FakeTravis, web_config)
        container.start()

        fake_url = "http://{}/fake_status".format(web_config['WEB_SERVER_ADDRESS'])
        with patch("examples.travis.URL_TEMPLATE", new=fake_url):
            yield

    @pytest.mark.usefixtures("fake_travis")
    def test_travis(self, container_factory, rabbit_config, web_session):

        from examples.travis import Travis

        container = container_factory(Travis, rabbit_config)
        container.start()

        with ServiceRpcProxy('travis_service', rabbit_config) as travis_rpc:
            status = travis_rpc.status_message("nameko", "nameko")
            assert "Project nameko/nameko" in status


class TestWebsocketRpc(object):

    def test_websocket_rpc(self, container_factory, web_config, websocket):

        from examples.websocket_rpc import WebsocketRpc

        container = container_factory(WebsocketRpc, web_config)
        container.start()

        ws = websocket()
        assert ws.rpc('echo', value=u"hellø") == u'hellø'


class TestConfig:

    def test_config_value_not_set(
        self, container_factory, web_config, web_session
    ):
        from examples.config_dependency_provider import Service

        container = container_factory(Service, web_config)
        container.start()

        res = web_session.get('/foo')
        assert res.status_code == 403
        assert res.text == "FeatureNotEnabled"

    def test_can_get_config_value(
        self, container_factory, web_config, web_session
    ):
        from examples.config_dependency_provider import Service

        config = web_config

        config["FOO_FEATURE_ENABLED"] = True

        container = container_factory(Service, config)
        container.start()

        res = web_session.get('/foo')
        assert res.status_code == 200
        assert res.text == "foo"


class TestAuth:

    @pytest.fixture
    def db(self):
        return {
            'matt': {
                'password': (
                    b'$2b$12$fZXR7Z1Eoyn0pfym8.'
                    b'LyRuIFabYj00ZzhdaJ0qoTLZs9w4fg3pKlK'
                ),
                'roles': [
                    'developer',
                ]
            },
            'susie': {
                'password': (
                    b'$2b$12$k4MVi9PcbSsOqONoj5vW9.'
                    b'pcQpB0xSjYkZcc6Ogr5nQ4MD8DRDiUK'
                ),
                'roles': [
                    'developer',
                    'admin'
                ]
            }
        }

    def test_authenticate(self, db):
        from examples.auth import Auth, JWT_SECRET

        worker_ctx = Mock(context_data={})
        dep = Auth.Api(db, worker_ctx)
        token = dep.authenticate("matt", "secret")
        jwt.decode(token, key=JWT_SECRET, verify=True)
        assert worker_ctx.context_data['auth'] == token

    def test_authenticate_bad_username(self, db):
        from examples.auth import Auth, Unauthenticated

        worker_ctx = Mock(context_data={})
        dep = Auth.Api(db, worker_ctx)
        with pytest.raises(Unauthenticated):
            dep.authenticate("angela", "secret")
        assert worker_ctx.context_data.get('auth') is None

    def test_authenticate_bad_password(self, db):
        from examples.auth import Auth, Unauthenticated

        worker_ctx = Mock(context_data={})
        dep = Auth.Api(db, worker_ctx)
        with pytest.raises(Unauthenticated):
            dep.authenticate("matt", "invalid")
        assert worker_ctx.context_data.get('auth') is None

    def test_authenticated_user_has_role(self, db):
        from examples.auth import Auth, JWT_SECRET

        token = jwt.encode(
            {'username': 'matt', 'roles': ['dev']}, key=JWT_SECRET
        )

        worker_ctx = Mock(context_data={'auth': token})
        dep = Auth.Api(db, worker_ctx)
        assert dep.has_role('dev')

    def test_authenticated_user_does_not_have_role(self, db):
        from examples.auth import Auth, JWT_SECRET

        token = jwt.encode(
            {'username': 'matt', 'roles': ['dev']}, key=JWT_SECRET
        )

        worker_ctx = Mock(context_data={'auth': token})
        dep = Auth.Api(db, worker_ctx)
        assert not dep.has_role('admin')

    def test_has_role_unauthenicated_user(self, db):
        from examples.auth import Auth, Unauthenticated

        worker_ctx = Mock(context_data={})
        dep = Auth.Api(db, worker_ctx)
        with pytest.raises(Unauthenticated):
            dep.has_role('admin')

    def test_has_role_invalid_token(self, db):
        from examples.auth import Auth

        worker_ctx = Mock(context_data={'auth': 'invalid-token'})
        dep = Auth.Api(db, worker_ctx)
        assert not dep.has_role('admin')

    def test_check_role(self, db):
        from examples.auth import Auth, Unauthorized, JWT_SECRET

        token = jwt.encode(
            {'username': 'matt', 'roles': ['dev']}, key=JWT_SECRET
        )

        worker_ctx = Mock(context_data={'auth': token})
        dep = Auth.Api(db, worker_ctx)

        assert dep.check_role('dev') is None
        with pytest.raises(Unauthorized):
            dep.check_role('admin')


class TestExpectedExceptions:

    def test_expected_exceptions(self, container_factory, rabbit_config):
        from examples.auth import JWT_SECRET
        from examples.expected_exceptions import Service

        container = container_factory(Service, rabbit_config)
        container.start()

        token = jwt.encode({"roles": []}, key=JWT_SECRET)

        with ServiceRpcProxy(
            "service", rabbit_config, context_data={"auth": token}
        ) as proxy:
            with pytest.raises(RemoteError) as exc:
                proxy.update(None)
            assert exc.value.exc_type == 'Unauthorized'

        admin_token = jwt.encode({"roles": ['admin']}, key=JWT_SECRET)

        with ServiceRpcProxy(
            "service", rabbit_config, context_data={"auth": admin_token}
        ) as proxy:
            with pytest.raises(RemoteError) as exc:
                proxy.update(None)
            assert exc.value.exc_type == 'TypeError'


class TestSensitiveArguments:

    def test_sensitive_arguments(self, container_factory, rabbit_config):
        from examples.auth import JWT_SECRET
        from examples.sensitive_arguments import Service

        container = container_factory(Service, rabbit_config)
        container.start()

        with ServiceRpcProxy("service", rabbit_config) as proxy:
            token = proxy.login("matt", "secret")
            jwt.decode(token, key=JWT_SECRET, verify=True)
            with pytest.raises(RemoteError) as exc:
                proxy.login("matt", "incorrect")
            assert exc.value.exc_type == "Unauthenticated"


class TestSqsReceive:

    @pytest.yield_fixture
    def sqs_client(self):
        with mock_sqs():
            client = boto3.client('sqs', region_name="eu-west-1")
            yield client

    @pytest.yield_fixture
    def queue(self, sqs_client):
        queue = sqs_client.create_queue(QueueName="nameko-sqs")
        url = queue['QueueUrl']
        yield url
        sqs_client.delete_queue(QueueUrl=url)

    def test_sqs_receive(self, queue, sqs_client, container_factory):

        from examples.sqs_receive import receive

        class Service:
            name = "sqs-service"

            @receive(queue)
            def handle(self, payload):
                return payload

        container = container_factory(Service, config={})
        container.start()

        with entrypoint_waiter(container, 'handle') as res:
            sqs_client.send_message(QueueUrl=queue, MessageBody="hello")
        assert res.get() == "hello"


class TestSqsSend:

    @pytest.yield_fixture
    def sqs_client(self):
        with mock_sqs():
            client = boto3.client('sqs', region_name="eu-west-1")
            yield client

    @pytest.yield_fixture
    def queue(self, sqs_client):
        queue = sqs_client.create_queue(QueueName="nameko-sqs")
        url = queue['QueueUrl']
        yield url
        sqs_client.delete_queue(QueueUrl=url)

    def test_sqs_send(self, queue, sqs_client, container_factory):

        from examples.sqs_send import SqsSend

        class Service:
            name = "sqs-service"

            send_message = SqsSend(queue)

            @dummy
            def method(self, payload):
                self.send_message(payload)

        container = container_factory(Service, config={})
        container.start()

        with entrypoint_hook(container, 'method') as send:
            send("hello")

        resp = sqs_client.receive_message(QueueUrl=queue)
        assert resp['Messages'][0]['Body'] == "hello"


class TestSqsService:

    @pytest.yield_fixture
    def sqs_client(self):
        with mock_sqs():
            client = boto3.client('sqs', region_name="eu-west-1")
            yield client

    @pytest.yield_fixture
    def queue(self, sqs_client):
        queue = sqs_client.create_queue(QueueName="nameko-sqs")
        url = queue['QueueUrl']
        yield url
        sqs_client.delete_queue(QueueUrl=url)

    def test_sqs_service(self, queue, sqs_client, container_factory):

        from examples.sqs_service import SqsService

        container = container_factory(SqsService, {})
        container.start()

        with entrypoint_waiter(container, 'handle_sqs_message') as res:
            sqs_client.send_message(QueueUrl=queue, MessageBody="hello")
        assert res.get() == "hello"
