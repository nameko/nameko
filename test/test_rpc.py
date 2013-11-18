from itertools import count
import pytest
import socket
import uuid

import eventlet
from kombu import Connection
from mock import patch, Mock, call

from nameko.dependencies import InjectionProvider, injection, DependencyFactory
from nameko.events import event_handler
from nameko.exceptions import RemoteError, MethodNotFound
from nameko.messaging import AMQP_URI_CONFIG_KEY, QueueConsumer
from nameko.rpc import (
    rpc, rpc_proxy, RpcConsumer, RpcProvider, ReplyListener)
from nameko.containers import (
    ServiceContainer, WorkerContext, WorkerContextBase, NAMEKO_DATA_KEYS)
from nameko.testing.utils import get_dependency


class ExampleError(Exception):
    pass


hello = object()
translations = {
    'en': {hello: 'hello'},
    'fr': {hello: 'bonjour'},
}


class Translator(InjectionProvider):

    def acquire_injection(self, worker_ctx):
        def translate(value):
            lang = worker_ctx.data['language']
            return translations[lang][value]
        return translate


@injection
def translator():
    return DependencyFactory(Translator)


class CustomWorkerContext(WorkerContextBase):
    data_keys = NAMEKO_DATA_KEYS + ('custom_header',)


class ExampleService(object):
    name = 'exampleservice'

    translate = translator()
    rpc_proxy = rpc_proxy('exampleservice')

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
        kwargs['interval_start'] = 0
        kwargs['interval_step'] = 0
        return super(FailingConnection, self).ensure(obj, wrapped, **kwargs)

    def connect(self, *args, **kwargs):
        if self.should_raise:
            self.failure_count += 1
            raise self.exc_type()
        return super(FailingConnection, self).connect(*args, **kwargs)


@pytest.yield_fixture
def get_rpc_exchange():
    with patch('nameko.rpc.get_rpc_exchange') as patched:
        yield patched


def test_rpc_consumer(get_rpc_exchange):

    container = Mock(spec=ServiceContainer)
    container.service_name = "exampleservice"

    exchange = Mock()
    get_rpc_exchange.return_value = exchange

    queue_consumer = Mock(spec=QueueConsumer)
    queue_consumer.bind("queue_consumer", container)

    consumer = RpcConsumer()
    consumer.queue_consumer = queue_consumer
    consumer.bind("rpc_consumer", container)

    provider = RpcProvider()
    provider.rpc_consumer = consumer
    provider.bind("rpcmethod", container)

    provider.prepare()
    consumer.prepare()
    queue_consumer.prepare()

    queue = consumer.queue
    assert queue.name == "rpc-exampleservice"
    assert queue.routing_key == "exampleservice.*"
    assert queue.exchange == exchange
    assert queue.durable

    queue_consumer.register_provider.assert_called_once_with(consumer)

    consumer.register_provider(provider)
    assert consumer._providers == set([provider])

    routing_key = "exampleservice.rpcmethod"
    assert consumer.get_provider_for_method(routing_key) == provider

    routing_key = "exampleservice.invalidmethod"
    with pytest.raises(MethodNotFound):
        consumer.get_provider_for_method(routing_key)

    consumer.unregister_provider(provider)
    assert consumer._providers == set()


def test_reply_listener(get_rpc_exchange):

    container = Mock(spec=ServiceContainer)
    container.service_name = "exampleservice"

    exchange = Mock()
    get_rpc_exchange.return_value = exchange

    queue_consumer = Mock(spec=QueueConsumer)
    queue_consumer.bind("queue_consumer", container)

    reply_listener = ReplyListener()
    reply_listener.queue_consumer = queue_consumer
    reply_listener.bind("reply_listener", container)

    forced_uuid = uuid.uuid4().hex

    with patch('nameko.rpc.uuid') as patched_uuid:
        patched_uuid.uuid4.return_value = forced_uuid

        reply_listener.prepare()
        queue_consumer.prepare()

        queue = reply_listener.queue
        assert queue.name == "rpc.reply-exampleservice-{}".format(forced_uuid)
        assert queue.exchange == exchange
        assert queue.exclusive

    queue_consumer.register_provider.assert_called_once_with(reply_listener)

    correlation_id = 1
    reply_event = reply_listener.get_reply_event(correlation_id)

    assert reply_listener._reply_events == {1: reply_event}

    message = Mock()
    message.properties.get.return_value = correlation_id
    reply_listener.handle_message("msg", message)

    queue_consumer.ack_message.assert_called_once_with(message)
    assert reply_event.ready()
    assert reply_event.wait() == "msg"

    assert reply_listener._reply_events == {}

    with patch('nameko.rpc._log') as log:
        reply_listener.handle_message("msg", message)
        assert log.debug.call_args == call(
            'Unknown correlation id: %s', correlation_id)

#==============================================================================
# INTEGRATION TESTS
#==============================================================================


def test_rpc_consumer_creates_single_consumer(container_factory, rabbit_config,
                                              rabbit_manager):
    container = container_factory(ExampleService, rabbit_config)
    container.start()

    # we should have 3 queues:
    #   * RPC requests
    #   * RPC replies
    #   * events
    vhost = rabbit_config['vhost']
    queues = rabbit_manager.get_queues(vhost)
    assert len(queues) == 3

    # each one should have one consumer
    rpc_queue = rabbit_manager.get_queue(vhost, "rpc-exampleservice")
    assert len(rpc_queue['consumer_details']) == 1
    evt_queue = rabbit_manager.get_queue(
        vhost, "evt-srcservice-eventtype--exampleservice.async_task")
    assert len(evt_queue['consumer_details']) == 1

    queue_names = [queue['name'] for queue in queues]
    reply_queue_names = [name for name in queue_names if 'rpc.reply' in name]
    assert len(reply_queue_names) == 1
    reply_queue_name = reply_queue_names[0]
    reply_queue = rabbit_manager.get_queue(vhost, reply_queue_name)
    assert len(reply_queue['consumer_details']) == 1

    # and share a single connection
    consumer_connection_names = set(
        queue['consumer_details'][0]['channel_details']['connection_name']
        for queue in [rpc_queue, evt_queue, reply_queue]
    )
    assert len(consumer_connection_names) == 1


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

    worker_ctx = WorkerContext(container, None, None,
                               data=context_data.copy())
    en_proxy = service_proxy_factory(container, "exampleservice", worker_ctx)

    context_data['language'] = 'fr'

    worker_ctx = WorkerContext(container, None, None,
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
    rpc_consumer = get_dependency(container, RpcConsumer)
    handle_message = rpc_consumer.handle_message

    with patch.object(rpc_consumer, 'handle_message') as patched_handler:
        def side_effect(body, message):
            headers.update(message.headers)  # extract message headers
            return handle_message(body, message)

        patched_handler.side_effect = side_effect

        worker_ctx = WorkerContext(container, None, None,
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
    rpc_consumer = get_dependency(container, RpcConsumer)
    handle_message = rpc_consumer.handle_message

    with patch.object(rpc_consumer, 'handle_message') as patched_handler:
        def side_effect(body, message):
            headers.update(message.headers)  # extract message headers
            return handle_message(body, message)

        patched_handler.side_effect = side_effect

        worker_ctx = CustomWorkerContext(container, None, None,
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

    uri = container.config[AMQP_URI_CONFIG_KEY]
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

    uri = container.config[AMQP_URI_CONFIG_KEY]
    conn = FailingConnection(uri)

    with patch("kombu.connection.ConnectionPool.new") as new_connection:
        new_connection.return_value = conn

        eventlet.spawn(proxy.task_a)
        with eventlet.Timeout(10):
            with pytest.raises(Exception) as exc_info:
                container.wait()
            assert type(exc_info.value) == socket.error

# test reply-to and correlation-id correct
