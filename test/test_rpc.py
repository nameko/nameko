from itertools import count
import socket
import uuid


import eventlet
from eventlet.event import Event
from kombu import Connection
from mock import patch, Mock, call
import pytest

from nameko.containers import (
    ServiceContainer, WorkerContextBase, NAMEKO_CONTEXT_KEYS)
from nameko.dependencies import InjectionProvider, injection, DependencyFactory
from nameko.events import event_handler
from nameko.exceptions import (
    RemoteError, MethodNotFound, UnknownService, IncorrectSignature)
from nameko.messaging import AMQP_URI_CONFIG_KEY, QueueConsumer
from nameko.rpc import (
    rpc, rpc_proxy, RpcConsumer, RpcProvider, ReplyListener)
from nameko.standalone.rpc import RpcProxy
from nameko.testing.services import entrypoint_hook
from nameko.testing.utils import get_dependency, wait_for_call


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
    context_keys = NAMEKO_CONTEXT_KEYS + ('custom_header',)


class ExampleService(object):
    name = 'exampleservice'

    translate = translator()
    example_rpc = rpc_proxy('exampleservice')
    unknown_rpc = rpc_proxy('unknown_service')

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
    def call_unknown(self):
        return self.unknown_rpc.any_method()

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
    with patch('nameko.rpc.get_rpc_exchange', autospec=True) as patched:
        yield patched


def test_rpc_consumer(get_rpc_exchange):

    container = Mock(spec=ServiceContainer)
    container.service_name = "exampleservice"
    container.service_cls = Mock(rpcmethod=lambda: None)

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

    with patch('nameko.rpc.uuid', autospec=True) as patched_uuid:
        patched_uuid.uuid4.return_value = forced_uuid

        reply_listener.prepare()
        queue_consumer.prepare()

        queue = reply_listener.queue
        assert queue.name == "rpc.reply-exampleservice-{}".format(forced_uuid)
        assert queue.exchange == exchange
        assert queue.routing_key == forced_uuid

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

    with patch('nameko.rpc._log', autospec=True) as log:
        reply_listener.handle_message("msg", message)
        assert log.debug.call_args == call(
            'Unknown correlation id: %s', correlation_id)


# =============================================================================
# INTEGRATION TESTS
# =============================================================================


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


def test_rpc_args_kwargs(container_factory, rabbit_config):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    with entrypoint_hook(container, 'echo') as echo:
        assert echo() == ((), {})
        assert echo("a", "b") == (("a", "b"), {})
        assert echo(foo="bar") == ((), {'foo': 'bar'})
        assert echo("arg", kwarg="kwarg") == (("arg",), {'kwarg': 'kwarg'})


def test_rpc_context_data(container_factory, rabbit_config):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    context_data = {
        'language': 'en',
        'auth_token': '123456789'
    }

    with entrypoint_hook(container, 'say_hello', context_data) as say_hello:
        assert say_hello() == "hello"

    context_data['language'] = 'fr'

    with entrypoint_hook(container, 'say_hello', context_data) as say_hello:
        assert say_hello() == "bonjour"


@pytest.mark.usefixtures("predictable_call_ids")
def test_rpc_headers(container_factory, rabbit_config):

    container = container_factory(ExampleService, rabbit_config)

    context_data = {
        'language': 'en',
        'bogus_header': '123456789'
    }

    headers = {}
    rpc_consumer = get_dependency(container, RpcConsumer)
    handle_message = rpc_consumer.handle_message

    with patch.object(
            rpc_consumer, 'handle_message', autospec=True) as patched_handler:
        def side_effect(body, message):
            headers.update(message.headers)  # extract message headers
            return handle_message(body, message)

        patched_handler.side_effect = side_effect
        container.start()

    # use a standalone rpc proxy to call exampleservice.say_hello()
    with RpcProxy("exampleservice", rabbit_config, context_data) as proxy:
        proxy.say_hello()

    # bogus_header dropped
    assert headers == {
        'nameko.language': 'en',
        'nameko.call_id_stack': ['standalone_rpc_proxy.call.0'],
    }


@pytest.mark.usefixtures("predictable_call_ids")
def test_rpc_custom_headers(container_factory, rabbit_config):
    container = container_factory(ExampleService, rabbit_config)

    context_data = {
        'language': 'en',
        'bogus_header': '123456789',
        'custom_header': 'specialvalue',
    }

    headers = {}
    rpc_consumer = get_dependency(container, RpcConsumer)
    handle_message = rpc_consumer.handle_message

    with patch.object(
            rpc_consumer, 'handle_message', autospec=True) as patched_handler:
        def side_effect(body, message):
            headers.update(message.headers)  # extract message headers
            return handle_message(body, message)

        patched_handler.side_effect = side_effect
        container.start()

    # use a standalone rpc proxy to call exampleservice.say_hello(),
    # with a worker context that enables "custom_header"
    with RpcProxy("exampleservice", rabbit_config,
                  context_data, CustomWorkerContext) as proxy:
        proxy.say_hello()

    # bogus_header dropped, custom_header present
    assert headers == {
        'nameko.language': 'en',
        'nameko.custom_header': 'specialvalue',
        'nameko.call_id_stack': ['standalone_rpc_proxy.call.0']
    }


def test_rpc_existing_method(container_factory, rabbit_config):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    with RpcProxy("exampleservice", rabbit_config) as proxy:
        assert proxy.task_a() == "result_a"
        assert proxy.task_b() == "result_b"


def test_rpc_incorrect_signature(container_factory, rabbit_config):

    class Service(object):

        @rpc
        def no_args(self):
            pass

        @rpc
        def args_only(self, a):
            pass

        @rpc
        def kwargs_only(self, a=None):
            pass

        @rpc
        def star_args(self, *args):
            pass

        @rpc
        def star_kwargs(self, **kwargs):
            pass

        @rpc
        def args_star_args(self, a, *args):
            pass

        @rpc
        def args_star_kwargs(self, a, **kwargs):
            pass

    container = container_factory(Service, rabbit_config)
    container.start()

    method_calls = [
        (('no_args', (), {}), None),
        (('no_args', ('bad arg',), {}),
            "no_args() takes no arguments (1 given)"),
        (('args_only', ('arg',), {}), None),
        (('args_only', (), {'a': 'arg'}), None),
        (('args_only', (), {'arg': 'arg'}),
            "args_only() got an unexpected keyword argument 'arg'"),
        (('kwargs_only', ('a',), {}), None),
        (('kwargs_only', (), {'a': 'arg'}), None),
        (('kwargs_only', (), {'arg': 'arg'}),
            "kwargs_only() got an unexpected keyword argument 'arg'"),
        (('star_args', ('a', 'b'), {}), None),
        (('star_args', (), {'c': 'c'}),
            "star_args() got an unexpected keyword argument 'c'"),
        (('args_star_args', ('a',), {}), None),
        (('args_star_args', ('a', 'b'), {}), None),
        (('args_star_args', (), {}),
            "args_star_args() takes at least 1 argument (0 given)"),
        (('args_star_args', (), {'c': 'c'}),
            "args_star_args() got an unexpected keyword argument 'c'"),
        (('args_star_kwargs', ('a',), {}), None),
        (('args_star_kwargs', ('a', 'b'), {}),
            "args_star_kwargs() takes exactly 1 argument (2 given)"),
        (('args_star_kwargs', ('a', 'b'), {'c': 'c'}),
            "args_star_kwargs() takes exactly 1 argument (3 given)"),
        (('args_star_kwargs', (), {}),
            "args_star_kwargs() takes exactly 1 argument (0 given)"),
    ]

    for signature, expected_error in method_calls:

        method_name, args, kwargs = signature

        with RpcProxy("service", rabbit_config) as proxy:
            method = getattr(proxy, method_name)

            if expected_error:
                with pytest.raises(IncorrectSignature) as exc_info:
                    method(*args, **kwargs)
                assert exc_info.value.message == expected_error
            else:
                method(*args, **kwargs)  # no raise


def test_rpc_missing_method(container_factory, rabbit_config, rabbit_manager):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    with RpcProxy("exampleservice", rabbit_config) as proxy:
        with pytest.raises(MethodNotFound) as exc_info:
            proxy.task_c()
    assert exc_info.value.message == "task_c"


def test_rpc_broken_method(container_factory, rabbit_config, rabbit_manager):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    with RpcProxy("exampleservice", rabbit_config) as proxy:
        with pytest.raises(RemoteError) as exc_info:
            proxy.broken()
    assert exc_info.value.exc_type == "ExampleError"


def test_rpc_unknown_service(container_factory, rabbit_config, rabbit_manager):
    container = container_factory(ExampleService, rabbit_config)
    container.start()

    with RpcProxy("exampleservice", rabbit_config) as proxy:
        # success
        assert proxy.task_a()

        # failure
        with pytest.raises(RemoteError) as exc_info:
            proxy.call_unknown()

    assert exc_info.value.exc_type == "UnknownService"


def test_rpc_unknown_service_standalone(rabbit_config, rabbit_manager):

    with RpcProxy("unknown_service", rabbit_config) as proxy:
        with pytest.raises(UnknownService) as exc_info:
            proxy.anything()

    assert exc_info.value._service_name == 'unknown_service'


def test_rpc_container_being_killed_retries(
        container_factory, rabbit_config, rabbit_manager):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    def wait_for_result():
        with RpcProxy("exampleservice", rabbit_config) as proxy:
            return proxy.task_a()

    container._being_killed = True

    rpc_provider = get_dependency(container, RpcProvider, name='task_a')

    with patch.object(
        rpc_provider,
        'rpc_consumer',
        wraps=rpc_provider.rpc_consumer,
    ) as wrapped_consumer:
        waiter = eventlet.spawn(wait_for_result)
        with wait_for_call(1, wrapped_consumer.requeue_message):
            pass  # wait until at least one message has been requeued
        assert not waiter.dead

    container._being_killed = False
    assert waiter.wait() == 'result_a'  # now completed


def test_rpc_responder_auto_retries(container_factory, rabbit_config,
                                    rabbit_manager):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    uri = container.config[AMQP_URI_CONFIG_KEY]
    conn = FailingConnection(uri, max_failure_count=2)

    path = "kombu.connection.ConnectionPool.new"
    with patch(path, autospec=True) as new_connection:
        new_connection.return_value = conn

        with RpcProxy("exampleservice", rabbit_config) as proxy:
            assert proxy.task_a() == "result_a"
        assert conn.failure_count == 2


def test_rpc_responder_eventual_failure(container_factory, rabbit_config,
                                        rabbit_manager):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    uri = container.config[AMQP_URI_CONFIG_KEY]
    conn = FailingConnection(uri)

    path = "kombu.connection.ConnectionPool.new"
    with patch(path, autospec=True) as new_connection:
        new_connection.return_value = conn

        with RpcProxy("exampleservice", rabbit_config) as proxy:
            eventlet.spawn(proxy.task_a)

        with eventlet.Timeout(10):
            with pytest.raises(Exception) as exc_info:
                container.wait()
            assert type(exc_info.value) == socket.error

# test reply-to and correlation-id correct


def test_rpc_consumer_sharing(container_factory, rabbit_config,
                              rabbit_manager):
    """ Verify that the RpcConsumer unregisters from the queueconsumer when
    the first provider unregisters itself. Otherwise it keeps consuming
    messages for the unregistered provider, raising MethodNotFound.
    """

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    task_a = get_dependency(container, RpcProvider, name="task_a")
    task_a_stop = task_a.stop

    task_b = get_dependency(container, RpcProvider, name="task_b")
    task_b_stop = task_b.stop

    task_a_stopped = Event()

    def patched_task_a_stop():
        task_a_stop()  # stop immediately
        task_a_stopped.send(True)

    def patched_task_b_stop():
        eventlet.sleep(2)  # stop after 2 seconds
        task_b_stop()

    with patch.object(task_b, 'stop', patched_task_b_stop), \
            patch.object(task_a, 'stop', patched_task_a_stop):

        # stop the container and wait for task_a to stop
        # task_b will still be in the process of stopping
        eventlet.spawn(container.stop)
        task_a_stopped.wait()

        # try to call task_a.
        # should timeout, rather than raising MethodNotFound
        with RpcProxy("exampleservice", rabbit_config) as proxy:
            with pytest.raises(eventlet.Timeout):
                with eventlet.Timeout(1):
                    proxy.task_a()

    # kill the container so we don't have to wait for task_b to stop
    container.kill()


def test_rpc_consumer_cannot_exit_with_providers(
        container_factory, rabbit_config, rabbit_manager):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    task_a = get_dependency(container, RpcProvider, name="task_a")

    def never_stops():
        while True:
            eventlet.sleep()

    with patch.object(task_a, 'stop', never_stops):
        with pytest.raises(eventlet.Timeout):
            with eventlet.Timeout(1):
                container.stop()

    # kill off task_a's misbehaving rpc provider
    container.kill()
