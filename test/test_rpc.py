from itertools import count
import pytest
import socket

from kombu import Connection
from mock import Mock, patch

from nameko.dependencies import AttributeDependency, dependency_decorator
from nameko.events import event_handler
from nameko.exceptions import RemoteError
from nameko.rpc import rpc, Service, get_rpc_consumer, RpcProvider


class ExampleError(Exception):
    pass


hello = object()
goodbye = object()
translations = {
    'en': {hello: 'hello', goodbye: 'goodbye'},
    'fr': {hello: 'bonjour', goodbye: 'au revoir'},
}


class Translator(AttributeDependency):

    def acquire_injection(self, worker_ctx):
        def translate(value):
            lang = worker_ctx.data['lang']
            return translations[lang][value]
        return translate


class CustomRpcProvider(RpcProvider):
    message_headers = ('lang', 'custom_header')


@dependency_decorator
def custom_rpc():
    return CustomRpcProvider()


class ExampleService(object):

    translate = Translator()

    @rpc
    def task_a(self, *args, **kwargs):
        print "task_a", args, kwargs
        return "result_a"

    @rpc
    def task_b(self, *args, **kwargs):
        print "task_b", args, kwargs
        return "result_b"

    @rpc
    def broken(self):
        raise ExampleError("broken")

    @rpc
    def echo(self, *args, **kwargs):
        return args, kwargs

    @rpc
    def say_hello(self):
        return self.translate(hello)

    @custom_rpc
    def say_goodbye(self):
        return self.translate(goodbye)

    @event_handler('srcservice', 'eventtype')
    def async_task(self):
        pass


class FailingConnection(Connection):
    exc_type = socket.error

    def __init__(self, *args, **kwargs):
        self.failure_count = 0
        self.max_failure_count = kwargs.pop('max_failure_count', count(0))
        return super(FailingConnection, self).__init__(*args, **kwargs)

    @property
    def should_raise(self):
        return self.failure_count < self.max_failure_count

    def maybe_raise(self, fun, *args, **kwargs):
        if self.should_raise:
            raise self.exc_type()
        return fun(*args, **kwargs)

    def ensure(self, obj, fun, **kwargs):
        def wrapped(*args, **kwargs):
            return self.maybe_raise(fun, *args, **kwargs)
        return super(FailingConnection, self).ensure(obj, wrapped, **kwargs)

    def connect(self, *args, **kwargs):
        if self.should_raise:
            self.failure_count += 1
            raise self.exc_type()
        return super(FailingConnection, self).connect(*args, **kwargs)


@pytest.fixture
def service_proxy_factory(request):
    def make_proxy(container, service_name, worker_ctx=None):
        if worker_ctx is None:
            worker_ctx = Mock(srv_ctx=container.ctx, data={})
        service_proxy = Service(service_name)
        proxy = service_proxy.acquire_injection(worker_ctx)
        return proxy
    return make_proxy


# test rpc proxy ...


def test_rpc_queue_and_connection_creation(container_factory, rabbit_config,
                                           rabbit_manager):
    container = container_factory(ExampleService, rabbit_config)
    container.start()

    # we should have a queue for RPC and a queue for events
    vhost = rabbit_config['vhost']
    queues = rabbit_manager.get_queues(vhost)
    assert len(queues) == 2

    # each one should have one consumer
    rpc_queue = rabbit_manager.get_queue(vhost, "rpc-exampleservice")
    assert len(rpc_queue['consumer_details']) == 1
    evt_queue = rabbit_manager.get_queue(
        vhost, "evt-srcservice-eventtype--exampleservice.async_task")
    assert len(evt_queue['consumer_details']) == 1

    # and both share a single connection
    connections = rabbit_manager.get_connections()
    assert len(connections) == 1


def test_rpc_args_kwargs(container_factory, rabbit_config,
                         service_proxy_factory):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    proxy = service_proxy_factory(container, "exampleservice")

    assert proxy.echo() == [[], {}]
    assert proxy.echo("a", "b") == [["a", "b"], {}]
    assert proxy.echo(foo="bar") == [[], {'foo': 'bar'}]
    assert proxy.echo("arg", kwarg="kwarg") == [["arg"], {'kwarg': 'kwarg'}]


def test_rpc_context_data(container_factory, rabbit_config,
                          service_proxy_factory):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    context_data = {
        'lang': 'en',
        'auth_token': '123456789'
    }

    worker_ctx = Mock(srv_ctx=container.ctx, data=context_data.copy())
    en_proxy = service_proxy_factory(container, "exampleservice", worker_ctx)

    context_data['lang'] = 'fr'

    worker_ctx = Mock(srv_ctx=container.ctx, data=context_data.copy())
    fr_proxy = service_proxy_factory(container, "exampleservice", worker_ctx)

    assert en_proxy.say_hello() == "hello"
    assert fr_proxy.say_hello() == "bonjour"


def test_rpc_headers(container_factory, rabbit_config,
                     service_proxy_factory):

    container = container_factory(ExampleService, rabbit_config)

    context_data = {
        'lang': 'en',
        'bogus_header': '123456789'
    }

    headers = {}
    rpc_consumer = get_rpc_consumer(container.ctx)
    handle_message = rpc_consumer.handle_message

    with patch.object(rpc_consumer, 'handle_message') as patched_handler:
        def side_effect(body, message):
            headers.update(message.headers)  # extract message headers
            return handle_message(body, message)

        patched_handler.side_effect = side_effect
        container.start()

    worker_ctx = Mock(srv_ctx=container.ctx, data=context_data.copy())
    proxy = service_proxy_factory(container, "exampleservice", worker_ctx)

    assert proxy.say_hello() == "hello"
    assert headers["_nameko_lang"] == "en"
    assert "_nameko_bogus_header" not in headers


def test_rpc_custom_headers(container_factory, rabbit_config,
                            service_proxy_factory):

    container = container_factory(ExampleService, rabbit_config)

    context_data = {
        'lang': 'en',
        'bogus_header': '123456789',
        'custom_header': 'specialvalue',
    }

    headers = {}
    rpc_consumer = get_rpc_consumer(container.ctx)
    handle_message = rpc_consumer.handle_message

    with patch.object(rpc_consumer, 'handle_message') as patched_handler:
        def side_effect(body, message):
            headers.update(message.headers)  # extract message headers
            return handle_message(body, message)

        patched_handler.side_effect = side_effect
        container.start()

    worker_ctx = Mock(srv_ctx=container.ctx, data=context_data.copy())
    proxy = service_proxy_factory(container, "exampleservice", worker_ctx)
    proxy.message_headers = CustomRpcProvider.message_headers

    assert proxy.say_hello() == "hello"
    assert headers["_nameko_lang"] == "en"
    assert "_nameko_bogus_header" not in headers
    assert headers["_nameko_custom_header"] == "specialvalue"


def test_rpc_existing_method(container_factory, rabbit_config, rabbit_manager,
                             service_proxy_factory):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    proxy = service_proxy_factory(container, "exampleservice")

    assert proxy.task_a() == "result_a"
    assert proxy.task_b() == "result_b"


def test_rpc_missing_method(container_factory, rabbit_config, rabbit_manager,
                            service_proxy_factory):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    proxy = service_proxy_factory(container, "exampleservice")

    with pytest.raises(RemoteError) as exc_info:
        proxy.task_c()
    assert exc_info.value.exc_type == "MethodNotFound"


def test_rpc_broken_method(container_factory, rabbit_config,
                           rabbit_manager, service_proxy_factory):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    proxy = service_proxy_factory(container, "exampleservice")

    with pytest.raises(RemoteError) as exc_info:
        proxy.broken()
    assert exc_info.value.exc_type == "ExampleError"


def test_rpc_responder_auto_retries(container_factory, rabbit_config,
                                    rabbit_manager, service_proxy_factory):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    proxy = service_proxy_factory(container, "exampleservice")
    uri = container.ctx.config['amqp_uri']
    conn = FailingConnection(uri, max_failure_count=2)

    with patch("kombu.connection.ConnectionPool.new") as new_connection:
        new_connection.return_value = conn

        assert proxy.task_a() == "result_a"
        assert conn.failure_count == 2


# test_rpc_responder_eventual_failure -- TODO
# test reply-to and correlation-id correct
