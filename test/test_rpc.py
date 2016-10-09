import uuid
import warnings

import eventlet
import pytest
from eventlet.event import Event
from mock import Mock, call, create_autospec, patch

from nameko.containers import ServiceContainer
from nameko.events import event_handler
from nameko.exceptions import (
    IncorrectSignature, MalformedRequest, MethodNotFound, RemoteError,
    UnknownService)
from nameko.extensions import DependencyProvider
from nameko.messaging import QueueConsumer
from nameko.rpc import ReplyListener, Rpc, RpcConsumer, RpcProxy, rpc
from nameko.standalone.rpc import ServiceRpcProxy
from nameko.testing.services import entrypoint_hook, restrict_entrypoints
from nameko.testing.utils import get_extension, wait_for_call


class ExampleError(Exception):
    pass


hello = object()
translations = {
    'en': {hello: 'hello'},
    'fr': {hello: 'bonjour'},
}


class Translator(DependencyProvider):

    def get_dependency(self, worker_ctx):
        def translate(value):
            lang = worker_ctx.data['language']
            return translations[lang][value]
        return translate


class WorkerErrorLogger(DependencyProvider):

    expected = {}
    unexpected = {}

    def worker_result(self, worker_ctx, result=None, exc_info=None):
        if exc_info is None:
            return  # nothing to do

        exc = exc_info[1]
        expected_exceptions = getattr(
            worker_ctx.entrypoint, 'expected_exceptions', ())

        if isinstance(exc, expected_exceptions):
            self.expected[worker_ctx.entrypoint.method_name] = type(exc)
        else:
            self.unexpected[worker_ctx.entrypoint.method_name] = type(exc)


class ExampleService(object):
    name = 'exampleservice'

    logger = WorkerErrorLogger()
    translate = Translator()
    example_rpc = RpcProxy('exampleservice')
    unknown_rpc = RpcProxy('unknown_service')

    @rpc
    def task_a(self, *args, **kwargs):
        return "result_a"

    @rpc
    def task_b(self, *args, **kwargs):
        return "result_b"

    @rpc(expected_exceptions=ExampleError)
    def broken(self):
        raise ExampleError("broken")

    @rpc(expected_exceptions=(KeyError, ValueError))
    def very_broken(self):
        raise AttributeError

    @rpc
    def call_async(self):
        res1 = self.example_rpc.task_a.call_async()
        res2 = self.example_rpc.task_b.call_async()
        res3 = self.example_rpc.echo.call_async()
        return [res2.result(), res1.result(), res3.result()]

    @rpc
    def deprecated_async(self):
        return self.example_rpc.echo.async().result()

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


@pytest.yield_fixture
def get_rpc_exchange():
    with patch('nameko.rpc.get_rpc_exchange', autospec=True) as patched:
        yield patched


@pytest.yield_fixture
def queue_consumer():
    replacement = create_autospec(QueueConsumer)
    with patch.object(QueueConsumer, 'bind') as mock_ext:
        mock_ext.return_value = replacement
        yield replacement


def test_rpc_consumer(get_rpc_exchange, queue_consumer, mock_container):

    container = mock_container
    container.shared_extensions = {}
    container.config = {}
    container.service_name = "exampleservice"
    container.service_cls = Mock(rpcmethod=lambda: None)

    exchange = Mock()
    get_rpc_exchange.return_value = exchange

    consumer = RpcConsumer().bind(container)

    entrypoint = Rpc().bind(container, "rpcmethod")
    entrypoint.rpc_consumer = consumer

    entrypoint.setup()
    consumer.setup()
    queue_consumer.setup()

    queue = consumer.queue
    assert queue.name == "rpc-exampleservice"
    assert queue.routing_key == "exampleservice.*"
    assert queue.exchange == exchange
    assert queue.durable

    queue_consumer.register_provider.assert_called_once_with(consumer)

    consumer.register_provider(entrypoint)
    assert consumer._providers == set([entrypoint])

    routing_key = "exampleservice.rpcmethod"
    assert consumer.get_provider_for_method(routing_key) == entrypoint

    routing_key = "exampleservice.invalidmethod"
    with pytest.raises(MethodNotFound):
        consumer.get_provider_for_method(routing_key)

    consumer.unregister_provider(entrypoint)
    assert consumer._providers == set()


def test_rpc_consumer_unregisters_if_no_providers(
    container_factory, rabbit_config
):
    class Service(object):
        name = "service"

        @rpc
        def method(self):
            pass

    container = container_factory(Service, rabbit_config)
    restrict_entrypoints(container)  # disable 'method' entrypoint

    rpc_consumer = get_extension(container, RpcConsumer)
    with patch.object(rpc_consumer, 'queue_consumer') as queue_consumer:
        rpc_consumer.stop()

    assert queue_consumer.unregister_provider.called
    assert rpc_consumer._unregistered_from_queue_consumer.ready()


def test_reply_listener(get_rpc_exchange, queue_consumer, mock_container):

    container = mock_container
    container.shared_extensions = {}
    container.config = {}
    container.service_name = "exampleservice"

    exchange = Mock()
    get_rpc_exchange.return_value = exchange

    reply_listener = ReplyListener().bind(container)

    forced_uuid = uuid.uuid4().hex

    with patch('nameko.rpc.uuid', autospec=True) as patched_uuid:
        patched_uuid.uuid4.return_value = forced_uuid

        reply_listener.setup()
        queue_consumer.setup()

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


def test_expected_exceptions(rabbit_config):
    container = ServiceContainer(ExampleService, rabbit_config)

    broken = get_extension(container, Rpc, method_name="broken")
    assert broken.expected_exceptions == ExampleError

    very_broken = get_extension(container, Rpc, method_name="very_broken")
    assert very_broken.expected_exceptions == (KeyError, ValueError)


# =============================================================================
# INTEGRATION TESTS
# =============================================================================

def test_expected_exceptions_integration(container_factory, rabbit_config):
    container = container_factory(ExampleService, rabbit_config)
    container.start()

    worker_logger = get_extension(container, WorkerErrorLogger)

    with entrypoint_hook(container, 'broken') as broken:
        with pytest.raises(ExampleError):
            broken()

    with entrypoint_hook(container, 'very_broken') as very_broken:
        with pytest.raises(AttributeError):
            very_broken()

    assert worker_logger.expected == {'broken': ExampleError}
    assert worker_logger.unexpected == {'very_broken': AttributeError}


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
        'otherheader': 'othervalue'
    }

    headers = {}
    rpc_consumer = get_extension(container, RpcConsumer)
    handle_message = rpc_consumer.handle_message

    with patch.object(
            rpc_consumer, 'handle_message', autospec=True) as patched_handler:
        def side_effect(body, message):
            headers.update(message.headers)  # extract message headers
            return handle_message(body, message)

        patched_handler.side_effect = side_effect
        container.start()

    # use a standalone rpc proxy to call exampleservice.say_hello()
    with ServiceRpcProxy(
        "exampleservice", rabbit_config, context_data
    ) as proxy:
        proxy.say_hello()

    # headers as per context data, plus call stack
    assert headers == {
        'nameko.language': 'en',
        'nameko.otherheader': 'othervalue',
        'nameko.call_id_stack': ['standalone_rpc_proxy.call.0'],
    }


def test_rpc_existing_method(container_factory, rabbit_config):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    with ServiceRpcProxy("exampleservice", rabbit_config) as proxy:
        assert proxy.task_a() == "result_a"
        assert proxy.task_b() == "result_b"


def test_async_rpc(container_factory, rabbit_config):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    with entrypoint_hook(container, 'call_async') as call_async:
        assert call_async() == ["result_b", "result_a", [[], {}]]


def test_async_rpc_deprecation_warning(container_factory, rabbit_config):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    with entrypoint_hook(container, 'deprecated_async') as call_async:

        # TODO: pytest.warns is not supported until pytest >= 2.8.0, whose
        # `testdir` plugin is not compatible with eventlet on python3 --
        # see https://github.com/mattbennett/eventlet-pytest-bug
        with warnings.catch_warnings(record=True) as ws:
            assert call_async() == [[], {}]
            assert len(ws) == 1
            assert issubclass(ws[-1].category, DeprecationWarning)
            assert "deprecated" in str(ws[-1].message)


def test_rpc_incorrect_signature(container_factory, rabbit_config):

    class Service(object):
        name = "service"

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
        (('no_args', (), {}), True),
        (('no_args', ('bad arg',), {}), False),
        (('args_only', ('arg',), {}), True),
        (('args_only', (), {'a': 'arg'}), True),
        (('args_only', (), {'arg': 'arg'}), False),
        (('kwargs_only', ('a',), {}), True),
        (('kwargs_only', (), {'a': 'arg'}), True),
        (('kwargs_only', (), {'arg': 'arg'}), False),
        (('star_args', ('a', 'b'), {}), True),
        (('star_args', (), {'c': 'c'}), False),
        (('args_star_args', ('a',), {}), True),
        (('args_star_args', ('a', 'b'), {}), True),
        (('args_star_args', (), {}), False),
        (('args_star_args', (), {'c': 'c'}), False),
        (('args_star_kwargs', ('a',), {}), True),
        (('args_star_kwargs', ('a', 'b'), {}), False),
        (('args_star_kwargs', ('a', 'b'), {'c': 'c'}), False),
        (('args_star_kwargs', (), {}), False),
    ]

    for signature, is_valid_call in method_calls:

        method_name, args, kwargs = signature

        with ServiceRpcProxy("service", rabbit_config) as proxy:
            method = getattr(proxy, method_name)

            if not is_valid_call:
                with pytest.raises(IncorrectSignature):
                    method(*args, **kwargs)
            else:
                method(*args, **kwargs)  # no raise


def test_rpc_missing_method(container_factory, rabbit_config):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    with ServiceRpcProxy("exampleservice", rabbit_config) as proxy:
        with pytest.raises(MethodNotFound) as exc_info:
            proxy.task_c()
    assert str(exc_info.value) == "task_c"


def test_rpc_invalid_message():
    entrypoint = Rpc()
    with pytest.raises(MalformedRequest) as exc:
        entrypoint.handle_message({'args': ()}, None)  # missing 'kwargs'
    assert 'Message missing `args` or `kwargs`' in str(exc)


def test_handle_message_raise_malformed_request(
        container_factory, rabbit_config):
    container = container_factory(ExampleService, rabbit_config)
    container.start()

    with pytest.raises(MalformedRequest):
        with patch('nameko.rpc.Rpc.handle_message') as handle_message:
            handle_message.side_effect = MalformedRequest('bad request')
            with ServiceRpcProxy("exampleservice", rabbit_config) as proxy:
                proxy.task_a()


def test_handle_message_raise_other_exception(
        container_factory, rabbit_config):
    container = container_factory(ExampleService, rabbit_config)
    container.start()

    with pytest.raises(RemoteError):
        with patch('nameko.rpc.Rpc.handle_message') as handle_message:
            handle_message.side_effect = Exception('broken')
            with ServiceRpcProxy("exampleservice", rabbit_config) as proxy:
                proxy.task_a()


def test_rpc_broken_method(container_factory, rabbit_config):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    with ServiceRpcProxy("exampleservice", rabbit_config) as proxy:
        with pytest.raises(RemoteError) as exc_info:
            proxy.broken()
    assert exc_info.value.exc_type == "ExampleError"


def test_rpc_unknown_service(container_factory, rabbit_config):
    container = container_factory(ExampleService, rabbit_config)
    container.start()

    with ServiceRpcProxy("exampleservice", rabbit_config) as proxy:
        # success
        assert proxy.task_a()

        # failure
        with pytest.raises(RemoteError) as exc_info:
            proxy.call_unknown()

    assert exc_info.value.exc_type == "UnknownService"


def test_rpc_unknown_service_standalone(rabbit_config):

    with ServiceRpcProxy("unknown_service", rabbit_config) as proxy:
        with pytest.raises(UnknownService) as exc_info:
            proxy.anything()

    assert exc_info.value._service_name == 'unknown_service'


def test_rpc_container_being_killed_retries(
        container_factory, rabbit_config):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    def wait_for_result():
        with ServiceRpcProxy("exampleservice", rabbit_config) as proxy:
            return proxy.task_a()

    container._being_killed = True

    rpc_provider = get_extension(container, Rpc, method_name='task_a')

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


def test_rpc_consumer_sharing(container_factory, rabbit_config,
                              rabbit_manager):
    """ Verify that the RpcConsumer unregisters from the queueconsumer when
    the first provider unregisters itself. Otherwise it keeps consuming
    messages for the unregistered provider, raising MethodNotFound.
    """

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    task_a = get_extension(container, Rpc, method_name="task_a")
    task_a_stop = task_a.stop

    task_b = get_extension(container, Rpc, method_name="task_b")
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
        with ServiceRpcProxy("exampleservice", rabbit_config) as proxy:
            with pytest.raises(eventlet.Timeout):
                with eventlet.Timeout(1):
                    proxy.task_a()

    # kill the container so we don't have to wait for task_b to stop
    container.kill()


def test_rpc_consumer_cannot_exit_with_providers(
        container_factory, rabbit_config):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    task_a = get_extension(container, Rpc, method_name="task_a")

    def never_stops():
        while True:
            eventlet.sleep()

    with patch.object(task_a, 'stop', never_stops):
        with pytest.raises(eventlet.Timeout):
            with eventlet.Timeout(1):
                container.stop()

    # kill off task_a's misbehaving rpc provider
    container.kill()
