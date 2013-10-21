from itertools import count
import pytest
import socket
import uuid

import eventlet
from kombu import Connection
from mock import patch

from nameko.dependencies import AttributeDependency
from nameko.events import event_handler
from nameko.exceptions import RemoteError

from nameko.messaging import AMQP_URI_CONFIG_KEY
from nameko.rpc import rpc, Service, get_rpc_consumer, RpcConsumer
from nameko.service import WorkerContext, WorkerContextBase, NAMEKO_DATA_KEYS


class ExampleError(Exception):
    pass


hello = object()
translations = {
    'en': {hello: 'hello'},
    'fr': {hello: 'bonjour'},
}


class Translator(AttributeDependency):

    def acquire_injection(self, worker_ctx):
        def translate(value):
            lang = worker_ctx.data['language']
            return translations[lang][value]
        return translate


class CustomWorkerContext(WorkerContextBase):
    data_keys = NAMEKO_DATA_KEYS + ('custom_header',)


class ExampleService(object):
    name = 'exampleservice'

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
            worker_ctx_cls = container.worker_ctx_cls
            worker_ctx = worker_ctx_cls(container.ctx, None, None, data={})
        service_proxy = Service(service_name)

        # manually add proxy as a dependency to get lifecycle management
        service_proxy.name = uuid.uuid4().hex
        container.dependencies.add(service_proxy)

        proxy = service_proxy.acquire_injection(worker_ctx)
        return proxy

    return make_proxy


# test rpc proxy ...


def test_rpc_consumer_creates_single_consumer(container_factory, rabbit_config,
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
    proxy = service_proxy_factory(container, "exampleservice")
    container.start()

    assert proxy.echo() == [[], {}]
    assert proxy.echo("a", "b") == [["a", "b"], {}]
    assert proxy.echo(foo="bar") == [[], {'foo': 'bar'}]
    assert proxy.echo("arg", kwarg="kwarg") == [["arg"], {'kwarg': 'kwarg'}]


def test_rpc_context_data(container_factory, rabbit_config,
                          service_proxy_factory):

    container = container_factory(ExampleService, rabbit_config)

    context_data = {
        'language': 'en',
        'auth_token': '123456789'
    }

    worker_ctx = WorkerContext(container.ctx, None, None,
                               data=context_data.copy())
    en_proxy = service_proxy_factory(container, "exampleservice", worker_ctx)

    context_data['language'] = 'fr'

    worker_ctx = WorkerContext(container.ctx, None, None,
                               data=context_data.copy())
    fr_proxy = service_proxy_factory(container, "exampleservice", worker_ctx)

    container.start()

    assert en_proxy.say_hello() == "hello"
    assert fr_proxy.say_hello() == "bonjour"


def test_rpc_headers(container_factory, rabbit_config,
                     service_proxy_factory):

    container = container_factory(ExampleService, rabbit_config)

    context_data = {
        'language': 'en',
        'bogus_header': '123456789'
    }

    headers = {}
    rpc_consumer = get_rpc_consumer(container.ctx, RpcConsumer)
    handle_message = rpc_consumer.handle_message

    with patch.object(rpc_consumer, 'handle_message') as patched_handler:
        def side_effect(body, message):
            headers.update(message.headers)  # extract message headers
            return handle_message(body, message)

        patched_handler.side_effect = side_effect

        worker_ctx = WorkerContext(container.ctx, None, None,
                                   data=context_data.copy())
        proxy = service_proxy_factory(container, "exampleservice", worker_ctx)
        container.start()

    assert proxy.say_hello() == "hello"
    assert headers == {'nameko.language': 'en'}  # bogus_header dropped


def test_rpc_custom_headers(container_factory, rabbit_config,
                            service_proxy_factory):

    container = container_factory(ExampleService, rabbit_config)

    context_data = {
        'language': 'en',
        'bogus_header': '123456789',
        'custom_header': 'specialvalue',
    }

    headers = {}
    rpc_consumer = get_rpc_consumer(container.ctx, RpcConsumer)
    handle_message = rpc_consumer.handle_message

    with patch.object(rpc_consumer, 'handle_message') as patched_handler:
        def side_effect(body, message):
            headers.update(message.headers)  # extract message headers
            return handle_message(body, message)

        patched_handler.side_effect = side_effect

        worker_ctx = CustomWorkerContext(container.ctx, None, None,
                                         data=context_data.copy())
        proxy = service_proxy_factory(container, "exampleservice", worker_ctx)
        container.start()

    assert proxy.say_hello() == "hello"
    # bogus_header dropped, custom_header present
    assert headers == {
        'nameko.language': 'en',
        'nameko.custom_header': 'specialvalue'
    }


def test_rpc_existing_method(container_factory, rabbit_config, rabbit_manager,
                             service_proxy_factory):

    container = container_factory(ExampleService, rabbit_config)
    proxy = service_proxy_factory(container, "exampleservice")
    container.start()

    assert proxy.task_a() == "result_a"
    assert proxy.task_b() == "result_b"


def test_rpc_missing_method(container_factory, rabbit_config, rabbit_manager,
                            service_proxy_factory):

    container = container_factory(ExampleService, rabbit_config)
    proxy = service_proxy_factory(container, "exampleservice")
    container.start()

    with pytest.raises(RemoteError) as exc_info:
        proxy.task_c()
    assert exc_info.value.exc_type == "MethodNotFound"


def test_rpc_broken_method(container_factory, rabbit_config,
                           rabbit_manager, service_proxy_factory):

    container = container_factory(ExampleService, rabbit_config)
    proxy = service_proxy_factory(container, "exampleservice")
    container.start()

    with pytest.raises(RemoteError) as exc_info:
        proxy.broken()
    assert exc_info.value.exc_type == "ExampleError"


def test_rpc_responder_auto_retries(container_factory, rabbit_config,
                                    rabbit_manager, service_proxy_factory):

    container = container_factory(ExampleService, rabbit_config)
    proxy = service_proxy_factory(container, "exampleservice")
    container.start()

    uri = container.ctx.config[AMQP_URI_CONFIG_KEY]
    conn = FailingConnection(uri, max_failure_count=2)

    with patch("kombu.connection.ConnectionPool.new") as new_connection:
        new_connection.return_value = conn

        assert proxy.task_a() == "result_a"
        assert conn.failure_count == 2


def test_rpc_responder_eventual_failure(container_factory, rabbit_config,
                                        rabbit_manager, service_proxy_factory):

    container = container_factory(ExampleService, rabbit_config)
    proxy = service_proxy_factory(container, "exampleservice")
    container.start()

    uri = container.ctx.config[AMQP_URI_CONFIG_KEY]
    conn = FailingConnection(uri)

    with patch("kombu.connection.ConnectionPool.new") as new_connection:
        new_connection.return_value = conn

        eventlet.spawn(proxy.task_a)
        with eventlet.Timeout(10):
            with pytest.raises(Exception) as exc_info:
                container.wait()
            assert type(exc_info.value) == socket.error

# test reply-to and correlation-id correct
