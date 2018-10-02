import uuid
from contextlib import contextmanager

import eventlet
import pytest
from eventlet.event import Event
from eventlet.semaphore import Semaphore
from greenlet import GreenletExit  # pylint: disable=E0611
from kombu.connection import Connection
from kombu.exceptions import OperationalError
from mock import Mock, call, patch
from six.moves import queue

from nameko.amqp.consume import Consumer
from nameko.constants import (
    AMQP_URI_CONFIG_KEY, HEARTBEAT_CONFIG_KEY, MAX_WORKERS_CONFIG_KEY
)
from nameko.containers import WorkerContext
from nameko.events import event_handler
from nameko.exceptions import (
    ContainerBeingKilled, IncorrectSignature, MalformedRequest, MethodNotFound,
    RemoteError, ReplyQueueExpiredWithPendingReplies, UnknownService
)
from nameko.extensions import DependencyProvider
from nameko.rpc import (
    Proxy, ReplyListener, Responder, Rpc, RpcConsumer, RpcProxy, rpc
)
from nameko.standalone.rpc import ServiceRpcProxy
from nameko.testing.services import dummy, entrypoint_hook, entrypoint_waiter
from nameko.testing.utils import get_extension
from nameko.testing.waiting import wait_for_call
from nameko.utils.retry import retry

from test import skip_if_no_toxiproxy


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


class ExampleService(object):
    name = 'exampleservice'

    translate = Translator()
    example_rpc = RpcProxy('exampleservice')
    unknown_rpc = RpcProxy('unknown_service')

    @rpc
    def task_a(self, *args, **kwargs):
        return "result_a"

    @rpc
    def task_b(self, *args, **kwargs):
        return "result_b"

    @rpc
    def call_async(self):
        res1 = self.example_rpc.task_a.call_async()
        res2 = self.example_rpc.task_b.call_async()
        res3 = self.example_rpc.echo.call_async()
        return [res2.result(), res1.result(), res3.result()]

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
        pass  # pragma: no cover

    @rpc
    def raises(self):
        raise ExampleError("error")


@pytest.yield_fixture
def get_rpc_exchange():
    with patch('nameko.rpc.get_rpc_exchange', autospec=True) as patched:
        yield patched


def test_rpc_consumer(get_rpc_exchange, mock_container):

    container = mock_container
    container.shared_extensions = {}
    container.config = {AMQP_URI_CONFIG_KEY: 'memory://localhost'}
    container.service_name = "exampleservice"
    container.service_cls = Mock(rpcmethod=lambda: None)

    exchange = Mock()
    get_rpc_exchange.return_value = exchange

    consumer = RpcConsumer().bind(container)

    entrypoint = Rpc().bind(container, "rpcmethod")
    entrypoint.rpc_consumer = consumer

    entrypoint.setup()
    consumer.setup()

    queue = consumer.queue
    assert queue.name == "rpc-exampleservice"
    assert queue.routing_key == "exampleservice.*"
    assert queue.exchange == exchange
    assert queue.durable

    consumer.register_provider(entrypoint)
    assert consumer._providers == set([entrypoint])

    routing_key = "exampleservice.rpcmethod"
    assert consumer.get_provider_for_method(routing_key) == entrypoint

    routing_key = "exampleservice.invalidmethod"
    with pytest.raises(MethodNotFound):
        consumer.get_provider_for_method(routing_key)

    consumer.unregister_provider(entrypoint)
    assert consumer._providers == set()


def test_reply_listener(get_rpc_exchange, mock_container):

    container = mock_container
    container.shared_extensions = {}
    container.config = {AMQP_URI_CONFIG_KEY: 'memory://localhost'}
    container.service_name = "exampleservice"

    exchange = Mock()
    get_rpc_exchange.return_value = exchange

    reply_listener = ReplyListener().bind(container)

    forced_uuid = uuid.uuid4().hex

    with patch('nameko.rpc.uuid', autospec=True) as patched_uuid:
        patched_uuid.uuid4.return_value = forced_uuid

        reply_listener.setup()

        queue = reply_listener.queue
        assert queue.name == "rpc.reply-exampleservice-{}".format(forced_uuid)
        assert queue.exchange == exchange
        assert queue.routing_key == forced_uuid

    correlation_id = 1
    get_response = reply_listener.register_for_reply(correlation_id)
    assert correlation_id in reply_listener.pending

    reply_event = reply_listener.pending[correlation_id]

    payload = {"result": "msg", "error": None}
    message = Mock()
    message.properties.get.return_value = correlation_id
    reply_listener.handle_message(payload, message)

    assert reply_listener.pending == {}

    assert reply_event.ready()
    assert reply_event.wait() == payload
    assert get_response() == payload

    with patch('nameko.rpc._log', autospec=True) as log:
        reply_listener.handle_message("msg", message)
        assert log.debug.call_args == call(
            'Unknown correlation id: %s', correlation_id)


class TestProxy(object):

    def test_getattr(self):
        publish = Mock()
        reply_listener = Mock()

        proxy = Proxy(publish, reply_listener)
        service_proxy = proxy.service
        assert proxy != service_proxy
        assert proxy.publish == service_proxy.publish
        assert proxy.register_for_reply == service_proxy.register_for_reply
        assert proxy.service_name is None
        assert proxy.method_name is None
        assert service_proxy.service_name == "service"
        assert service_proxy.method_name is None

        method1_proxy = service_proxy.method1
        assert service_proxy != method1_proxy
        assert service_proxy.publish == method1_proxy.publish
        assert service_proxy.register_for_reply == (
            method1_proxy.register_for_reply
        )
        assert service_proxy.service_name == "service"
        assert service_proxy.method_name is None
        assert method1_proxy.service_name == "service"
        assert method1_proxy.method_name == "method1"

        method2_proxy = service_proxy.method2

        method2_proxy = service_proxy.method2
        assert service_proxy != method2_proxy
        assert service_proxy.publish == method2_proxy.publish
        assert service_proxy.register_for_reply == (
            method2_proxy.register_for_reply
        )
        assert service_proxy.service_name == "service"
        assert service_proxy.method_name is None
        assert method2_proxy.service_name == "service"
        assert method2_proxy.method_name == "method2"

        assert method1_proxy != method2_proxy

        with pytest.raises(AttributeError):
            method1_proxy.attr
        with pytest.raises(AttributeError):
            method2_proxy.attr

    def test_identifier(self):
        publish = Mock()
        register_for_reply = Mock()

        proxy = Proxy(publish, register_for_reply)
        service_proxy = proxy.service
        method1_proxy = service_proxy.method1
        method2_proxy = service_proxy.method2

        assert proxy.identifier == "*.*"
        assert service_proxy.identifier == "service.*"
        assert method1_proxy.identifier == "service.method1"
        assert method2_proxy.identifier == "service.method2"

    def test_dict_access(self):
        publish = Mock()
        register_for_reply = Mock()

        proxy = Proxy(publish, register_for_reply)

        assert proxy['service'].identifier == "service.*"
        assert proxy['service']['method'].identifier == "service.method"

    def test_cannot_invoke_unspecified_proxy(self):
        publish = Mock()
        register_for_reply = Mock()

        proxy = Proxy(publish, register_for_reply)

        with pytest.raises(ValueError):
            proxy()

        with pytest.raises(ValueError):
            proxy.service()


# =============================================================================
# INTEGRATION TESTS
# =============================================================================


def test_rpc_consumer_connections(
    container_factory, rabbit_config, rabbit_manager
):

    class ExampleService(object):
        name = 'exampleservice'

        other_rpc = RpcProxy('other')

        @rpc
        def echo(self, arg):
            return arg  # pragma: no cover

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    # we should have 2 queues:
    #   * RPC requests
    #   * RPC replies
    vhost = rabbit_config['vhost']
    queues = rabbit_manager.get_queues(vhost)
    assert len(queues) == 2

    # each one should have one consumer
    rpc_queue = rabbit_manager.get_queue(vhost, "rpc-exampleservice")
    assert len(rpc_queue['consumer_details']) == 1

    queue_names = [queue['name'] for queue in queues]
    reply_queue_names = [name for name in queue_names if 'rpc.reply' in name]
    assert len(reply_queue_names) == 1
    reply_queue_name = reply_queue_names[0]
    reply_queue = rabbit_manager.get_queue(vhost, reply_queue_name)
    assert len(reply_queue['consumer_details']) == 1

    # with one connection each
    # (connection sharing not supported until we migrate away from kombu)
    consumer_connection_names = set(
        queue['consumer_details'][0]['channel_details']['connection_name']
        for queue in [rpc_queue, reply_queue]
    )
    assert len(consumer_connection_names) == 2


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
    container.start()

    context_data = {
        'language': 'en',
        'otherheader': 'othervalue'
    }

    headers = {}

    rpc_consumer = get_extension(container, RpcConsumer)

    with patch.object(
        rpc_consumer, 'handle_result', wraps=rpc_consumer.handle_result
    ) as patched:

        patched.side_effect = lambda msg, *args: headers.update(msg.headers)

        # use a standalone rpc proxy to call exampleservice.say_hello()
        with ServiceRpcProxy(
            "exampleservice", rabbit_config, context_data
        ) as proxy:
            assert proxy.say_hello() == "hello"

        # headers as per context data, plus call stack
        assert headers == {
            'nameko.language': 'en',
            'nameko.otherheader': 'othervalue',
            'nameko.call_id_stack': ['standalone_rpc_proxy.0.0']
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
        (('star_kwargs', (), {'c': 'c'}), True),
        (('star_kwargs', ('a', 'b'), {}), False),
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


def test_rpc_method_that_raises(container_factory, rabbit_config):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    with ServiceRpcProxy("exampleservice", rabbit_config) as proxy:
        with pytest.raises(RemoteError) as exc_info:
            proxy.raises()
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


class TestContainerBeingKilled(object):

    @pytest.yield_fixture
    def service_rpc(self, rabbit_config):
        with ServiceRpcProxy("service", rabbit_config) as proxy:
            yield proxy

    def test_container_killed(
        self, container_factory, rabbit_config, service_rpc
    ):
        class Service(object):
            name = "service"

            @rpc
            def method(self):
                pass  # pragma: no cover

        container = container_factory(Service, rabbit_config)
        container.start()

        # check message is requeued if container throws ContainerBeingKilled
        with patch.object(container, 'spawn_worker') as spawn_worker:
            spawn_worker.side_effect = ContainerBeingKilled()

            with wait_for_call(Consumer, 'requeue_message'):
                service_rpc.method.call_async()


def test_rpc_consumer_sharing(container_factory, rabbit_config,
                              rabbit_manager):
    """ Verify that the RpcConsumer stops when the first provider unregisters
    itself. Otherwise it would keep consuming messages for the unregistered
    provider, raising MethodNotFound.
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
    container_factory, rabbit_config
):
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


@patch('nameko.rpc.RPC_REPLY_QUEUE_TTL', new=100)
def test_reply_queue_removed_on_expiry(
    container_factory, rabbit_config, rabbit_manager
):
    def list_queues():
        vhost = rabbit_config['vhost']
        return [
            queue['name']
            for queue in rabbit_manager.get_queues(vhost=vhost)
        ]

    class Service(object):
        name = "service"

        delegate_rpc = RpcProxy('delegate')

        @dummy
        def method(self, arg):
            pass  # pragma: no cover

    container = container_factory(Service, rabbit_config)
    container.start()

    reply_queue = [
        queue for queue in list_queues() if "rpc.reply" in queue
    ][0]

    container.stop()
    eventlet.sleep(0.15)  # sleep for >TTL
    assert reply_queue not in list_queues()


@patch('nameko.rpc.RPC_REPLY_QUEUE_TTL', new=200)
def test_async_wait_longer_than_expiry(container_factory, rabbit_config):
    """ RpcProxy ReplyListener consumer runs in a thread and prevents the
    reply queue from expiring. Contrast to the single-threaded equivalent
    in nameko.standalone.rpc.
    """

    class Service(object):
        name = "service"

        delegate_rpc = RpcProxy('delegate')

        @dummy
        def echo(self, arg):
            res = self.delegate_rpc.echo.call_async(arg)
            eventlet.sleep(0.4)  # sleep for 2x TTL
            return res.result()

    class DelegateService(object):
        name = "delegate"

        @rpc
        def echo(self, arg):
            return arg

    container = container_factory(Service, rabbit_config)
    container.start()

    delegate_container = container_factory(DelegateService, rabbit_config)
    delegate_container.start()

    with entrypoint_hook(container, 'echo') as echo:
        assert echo('foo') == 'foo'


@patch('nameko.standalone.rpc.RPC_REPLY_QUEUE_TTL', new=200)
def test_request_longer_than_expiry(container_factory, rabbit_config):

    class Service(object):
        name = "service"

        delegate_rpc = RpcProxy('delegate')

        @dummy
        def echo(self, arg):
            return self.delegate_rpc.echo(arg)

    class DelegateService(object):
        name = "delegate"

        @rpc
        def echo(self, arg):
            eventlet.sleep(0.4)  # sleep for 2x TTL
            return arg

    container = container_factory(Service, rabbit_config)
    container.start()

    delegate_container = container_factory(DelegateService, rabbit_config)
    delegate_container.start()

    with entrypoint_hook(container, 'echo') as echo:
        assert echo('foo') == 'foo'


@skip_if_no_toxiproxy
class TestDisconnectedWhileWaitingForReply(object):  # pragma: no cover

    @pytest.yield_fixture(autouse=True)
    def fast_reconnects(self):

        @contextmanager
        def establish_connection(self):
            with self.create_connection() as conn:
                conn.ensure_connection(
                    self.on_connection_error,
                    self.connect_max_retries,
                    interval_start=0.1,
                    interval_step=0.1)
                yield conn

        with patch.object(
            Consumer, 'establish_connection', new=establish_connection
        ):
            yield

    @pytest.yield_fixture
    def fast_expiry(self):
        with patch('nameko.rpc.RPC_REPLY_QUEUE_TTL', new=100):
            yield

    @pytest.yield_fixture
    def toxic_reply_listener(self, toxiproxy):
        with patch.object(ReplyListener, 'amqp_uri', new=toxiproxy.uri):
            yield

    @pytest.fixture(autouse=True)
    def container(
        self, container_factory, rabbit_config, rabbit_manager, toxiproxy,
        fast_expiry, toxic_reply_listener
    ):

        def enable_after_queue_expires():
            eventlet.sleep(1)
            toxiproxy.enable()

        class Service(object):
            name = "service"

            delegate_rpc = RpcProxy('delegate')

            @dummy
            def sleep(self):
                return self.delegate_rpc.sleep()

        class DelegateService(object):
            name = "delegate"

            @rpc
            def sleep(self):
                toxiproxy.disable()
                eventlet.spawn_n(enable_after_queue_expires)
                return "OK"

        config = rabbit_config

        container = container_factory(Service, config)
        container.start()

        delegate_container = container_factory(DelegateService, config)
        delegate_container.start()

        return container

    def test_reply_queue_removed_while_disconnected_with_pending_reply(
        self, container, toxiproxy
    ):
        with entrypoint_hook(container, 'sleep') as hook:
            with pytest.raises(ReplyQueueExpiredWithPendingReplies):
                hook()


@skip_if_no_toxiproxy
class TestReplyListenerDisconnections(object):
    """ Test and demonstrate behaviour under poor network conditions.
    """
    @pytest.yield_fixture(autouse=True)
    def fast_reconnects(self):

        @contextmanager
        def establish_connection(self):
            with self.create_connection() as conn:
                conn.ensure_connection(
                    self.on_connection_error,
                    self.connect_max_retries,
                    interval_start=0.1,
                    interval_step=0.1)
                yield conn

        with patch.object(
            Consumer, 'establish_connection', new=establish_connection
        ):
            yield

    @pytest.yield_fixture
    def toxic_reply_listener(self, toxiproxy):
        with patch.object(ReplyListener, 'amqp_uri', new=toxiproxy.uri):
            yield

    @pytest.fixture(autouse=True)
    def container(
        self, container_factory, rabbit_config, toxiproxy, toxic_reply_listener
    ):
        class Service(object):
            name = "service"

            delegate_rpc = RpcProxy('delegate')

            @dummy
            def echo(self, arg):
                return self.delegate_rpc.echo(arg)

        class DelegateService(object):
            name = "delegate"

            @rpc
            def echo(self, arg):
                return arg

        # very fast heartbeat
        config = rabbit_config
        config[HEARTBEAT_CONFIG_KEY] = 2  # seconds

        container = container_factory(Service, config)
        container.start()

        delegate_container = container_factory(DelegateService, config)
        delegate_container.start()

        return container

    def test_normal(self, container):
        msg = "foo"
        with entrypoint_hook(container, 'echo') as echo:
            assert echo(msg) == msg

    def test_down(self, container, toxiproxy):
        """ Verify we detect and recover from closed sockets.

        This failure mode closes the socket between the consumer and the
        rabbit broker.

        Attempting to read from the closed socket raises a socket.error
        and the connection is re-established.
        """
        reply_listener = get_extension(container, ReplyListener)

        def reset(args, kwargs, result, exc_info):
            toxiproxy.enable()
            return True

        with wait_for_call(
            reply_listener.consumer, 'on_connection_error', callback=reset
        ):
            toxiproxy.disable()

        # connection re-established
        msg = "foo"
        with entrypoint_hook(container, 'echo') as echo:
            assert echo(msg) == msg

    def test_upstream_timeout(self, container, toxiproxy):
        """ Verify we detect and recover from sockets timing out.

        This failure mode means that the socket between the consumer and the
        rabbit broker times out after `timeout` milliseconds and then closes.

        Attempting to read from the socket after it's closed raises a
        socket.error and the connection will be re-established. If `timeout`
        is longer than twice the heartbeat interval, the behaviour is the same
        as in `test_upstream_blackhole` below.
        """
        reply_listener = get_extension(container, ReplyListener)

        def reset(args, kwargs, result, exc_info):
            toxiproxy.reset_timeout()
            return True

        with wait_for_call(
            reply_listener.consumer, 'on_connection_error', callback=reset
        ):
            toxiproxy.set_timeout(timeout=100)

        # connection re-established
        msg = "foo"
        with entrypoint_hook(container, 'echo') as echo:
            assert echo(msg) == msg

    def test_upstream_blackhole(self, container, toxiproxy):
        """ Verify we detect and recover from sockets losing data.

        This failure mode means that all data sent from the consumer to the
        rabbit broker is lost, but the socket remains open.

        Heartbeats sent from the consumer are not received by the broker. After
        two beats are missed the broker closes the connection, and subsequent
        reads from the socket raise a socket.error, so the connection is
        re-established.
        """
        reply_listener = get_extension(container, ReplyListener)

        def reset(args, kwargs, result, exc_info):
            toxiproxy.reset_timeout()
            return True

        with wait_for_call(
            reply_listener.consumer, 'on_connection_error', callback=reset
        ):
            toxiproxy.set_timeout(timeout=0)

        # connection re-established
        msg = "foo"
        with entrypoint_hook(container, 'echo') as echo:
            assert echo(msg) == msg

    def test_downstream_timeout(self, container, toxiproxy):
        """ Verify we detect and recover from sockets timing out.

        This failure mode means that the socket between the rabbit broker and
        the consumer times out after `timeout` milliseconds and then closes.

        Attempting to read from the socket after it's closed raises a
        socket.error and the connection will be re-established. If `timeout`
        is longer than twice the heartbeat interval, the behaviour is the same
        as in `test_downstream_blackhole` below, except that the consumer
        cancel will eventually (`timeout` milliseconds) raise a socket.error,
        which is ignored, allowing the teardown to continue.

        See :meth:`kombu.messsaging.Consumer.__exit__`
        """
        reply_listener = get_extension(container, ReplyListener)

        def reset(args, kwargs, result, exc_info):
            toxiproxy.reset_timeout()
            return True

        with wait_for_call(
            reply_listener.consumer, 'on_connection_error', callback=reset
        ):
            toxiproxy.set_timeout(stream="downstream", timeout=100)

        # connection re-established
        msg = "foo"
        with entrypoint_hook(container, 'echo') as echo:
            assert echo(msg) == msg

    def test_downstream_blackhole(
        self, container, toxiproxy
    ):  # pragma: no cover
        """ Verify we detect and recover from sockets losing data.

        This failure mode means that all data sent from the rabbit broker to
        the consumer is lost, but the socket remains open.

        Heartbeat acknowledgements from the broker are not received by the
        consumer. After two beats are missed the consumer raises a "too many
        heartbeats missed" error.

        Cancelling the consumer requests an acknowledgement from the broker,
        which is swallowed by the socket. There is no timeout when reading
        the acknowledgement so this hangs forever.

        See :meth:`kombu.messsaging.Consumer.__exit__`
        """
        pytest.skip("skip until kombu supports recovery in this scenario")

        reply_listener = get_extension(container, ReplyListener)

        def reset(args, kwargs, result, exc_info):
            toxiproxy.reset_timeout()
            return True

        with wait_for_call(
            reply_listener.consumer, 'on_connection_error', callback=reset
        ):
            toxiproxy.set_timeout(stream="downstream", timeout=0)

        # connection re-established
        msg = "foo"
        with entrypoint_hook(container, 'echo') as echo:
            assert echo(msg) == msg


@skip_if_no_toxiproxy
class TestRpcConsumerDisconnections(object):
    """ Test and demonstrate behaviour under poor network conditions.
    """
    @pytest.yield_fixture(autouse=True)
    def fast_reconnects(self):

        @contextmanager
        def establish_connection(self):
            with self.create_connection() as conn:
                conn.ensure_connection(
                    self.on_connection_error,
                    self.connect_max_retries,
                    interval_start=0.1,
                    interval_step=0.1)
                yield conn

        with patch.object(
            Consumer, 'establish_connection', new=establish_connection
        ):
            yield

    @pytest.yield_fixture
    def toxic_rpc_consumer(self, toxiproxy):
        with patch.object(RpcConsumer, 'amqp_uri', new=toxiproxy.uri):
            yield

    @pytest.yield_fixture(autouse=True)
    def nontoxic_responder(self, rabbit_config):
        def replacement_constructor(amqp_uri, *args, **kw):
            return Responder(rabbit_config[AMQP_URI_CONFIG_KEY], *args, **kw)
        with patch('nameko.rpc.Responder', wraps=replacement_constructor):
            yield

    @pytest.yield_fixture
    def service_rpc(self, rabbit_config):
        with ServiceRpcProxy('service', rabbit_config) as proxy:
            yield proxy

    @pytest.fixture
    def lock(self):
        return Semaphore()

    @pytest.fixture
    def tracker(self):
        return Mock()

    @pytest.fixture(autouse=True)
    def container(
        self, container_factory, rabbit_config, toxic_rpc_consumer, lock,
        tracker
    ):

        class Service(object):
            name = "service"

            @rpc
            def echo(self, arg):
                lock.acquire()
                lock.release()
                tracker(arg)
                return arg

        # very fast heartbeat
        config = rabbit_config
        config[HEARTBEAT_CONFIG_KEY] = 2  # seconds

        container = container_factory(Service, config)
        container.start()

        # we have to let the container connect before disconnecting
        # otherwise we end up in retry_over_time trying to make the
        # initial connection; we get stuck there because it has a
        # function-local copy of "on_connection_error" that is never patched
        eventlet.sleep(.05)

        return container

    def test_normal(self, container, service_rpc):
        assert service_rpc.echo("foo") == "foo"

    def test_down(self, container, service_rpc, toxiproxy):
        """ Verify we detect and recover from closed sockets.

        This failure mode closes the socket between the consumer and the
        rabbit broker.

        Attempting to read from the closed socket raises a socket.error
        and the connection is re-established.
        """
        rpc_consumer = get_extension(container, RpcConsumer)

        def reset(args, kwargs, result, exc_info):
            toxiproxy.enable()
            return True

        with wait_for_call(
            rpc_consumer.consumer, 'on_connection_error', callback=reset
        ):
            toxiproxy.disable()

        # connection re-established
        assert service_rpc.echo("foo") == "foo"

    def test_upstream_timeout(self, container, service_rpc, toxiproxy):
        """ Verify we detect and recover from sockets timing out.

        This failure mode means that the socket between the consumer and the
        rabbit broker times out after `timeout` milliseconds and then closes.

        Attempting to read from the socket after it's closed raises a
        socket.error and the connection will be re-established. If `timeout`
        is longer than twice the heartbeat interval, the behaviour is the same
        as in `test_upstream_blackhole` below.
        """
        rpc_consumer = get_extension(container, RpcConsumer)

        def reset(args, kwargs, result, exc_info):
            toxiproxy.reset_timeout()
            return True

        with wait_for_call(
            rpc_consumer.consumer, 'on_connection_error', callback=reset
        ):
            toxiproxy.set_timeout(timeout=100)

        # connection re-established
        assert service_rpc.echo("foo") == "foo"

    def test_upstream_blackhole(self, container, service_rpc, toxiproxy):
        """ Verify we detect and recover from sockets losing data.

        This failure mode means that all data sent from the consumer to the
        rabbit broker is lost, but the socket remains open.

        Heartbeats sent from the consumer are not received by the broker. After
        two beats are missed the broker closes the connection, and subsequent
        reads from the socket raise a socket.error, so the connection is
        re-established.
        """
        rpc_consumer = get_extension(container, RpcConsumer)

        def reset(args, kwargs, result, exc_info):
            toxiproxy.reset_timeout()
            return True

        with wait_for_call(
            rpc_consumer.consumer, 'on_connection_error', callback=reset
        ):
            toxiproxy.set_timeout(timeout=0)

        # connection re-established
        assert service_rpc.echo("foo") == "foo"

    def test_downstream_timeout(self, container, service_rpc, toxiproxy):
        """ Verify we detect and recover from sockets timing out.

        This failure mode means that the socket between the rabbit broker and
        the consumer times out after `timeout` milliseconds and then closes.

        Attempting to read from the socket after it's closed raises a
        socket.error and the connection will be re-established. If `timeout`
        is longer than twice the heartbeat interval, the behaviour is the same
        as in `test_downstream_blackhole` below, except that the consumer
        cancel will eventually (`timeout` milliseconds) raise a socket.error,
        which is ignored, allowing the teardown to continue.

        See :meth:`kombu.messsaging.Consumer.__exit__`
        """
        rpc_consumer = get_extension(container, RpcConsumer)

        def reset(args, kwargs, result, exc_info):
            toxiproxy.reset_timeout()
            return True

        with wait_for_call(
            rpc_consumer.consumer, 'on_connection_error', callback=reset
        ):
            toxiproxy.set_timeout(stream="downstream", timeout=100)

        # connection re-established
        assert service_rpc.echo("foo") == "foo"

    def test_downstream_blackhole(
        self, container, service_rpc, toxiproxy
    ):  # pragma: no cover
        """ Verify we detect and recover from sockets losing data.

        This failure mode means that all data sent from the rabbit broker to
        the consumer is lost, but the socket remains open.

        Heartbeat acknowledgements from the broker are not received by the
        consumer. After two beats are missed the consumer raises a "too many
        heartbeats missed" error.

        Cancelling the consumer requests an acknowledgement from the broker,
        which is swallowed by the socket. There is no timeout when reading
        the acknowledgement so this hangs forever.

        See :meth:`kombu.messsaging.Consumer.__exit__`
        """
        pytest.skip("skip until kombu supports recovery in this scenario")

        rpc_consumer = get_extension(container, RpcConsumer)

        def reset(args, kwargs, result, exc_info):
            toxiproxy.reset_timeout()
            return True

        with wait_for_call(
            rpc_consumer.consumer, 'on_connection_error', callback=reset
        ):
            toxiproxy.set_timeout(stream="downstream", timeout=0)

        # connection re-established
        assert service_rpc.echo("foo") == "foo"

    def test_message_ack_regression(
        self, container, service_rpc, toxiproxy, lock, tracker
    ):
        """ Regression for https://github.com/nameko/nameko/issues/511
        """
        # prevent workers from completing
        lock.acquire()

        # fire entrypoint and block the worker;
        # break connection while the worker is active, then release worker
        with entrypoint_waiter(container, 'echo') as result:
            res = service_rpc.echo.call_async("msg1")
            while not lock._waiters:
                eventlet.sleep()  # pragma: no cover
            toxiproxy.disable()
            # allow connection to close before releasing worker
            eventlet.sleep(.1)
            lock.release()

        # entrypoint will return and reply will be received,
        # but the initiating message will not be ack'd
        assert result.get() == "msg1"
        assert res.result() == "msg1"

        # enabling connection will re-deliver the initiating message
        # and it will be processed again
        with entrypoint_waiter(container, 'echo') as result:
            toxiproxy.enable()
        assert result.get() == "msg1"

        # connection re-established, container should work again
        assert service_rpc.echo("msg2") == "msg2"


@skip_if_no_toxiproxy
class TestProxyDisconnections(object):
    """ Test and demonstrate behaviour under poor network conditions.

    Publisher confirms must be enabled for some of these tests to pass. Without
    confirms, previously used but now dead connections will accept writes
    without raising. These tests are skipped in this scenario.

    Note that publisher confirms do not protect against sockets that remain
    open but do not deliver messages (i.e. `toxiproxy.set_timeout(0)`).
    This can only be mitigated with AMQP heartbeats (not yet supported)
    """

    @pytest.fixture(autouse=True)
    def server_container(self, container_factory, rabbit_config):

        class Service(object):
            name = "server"

            @rpc
            def echo(self, arg):
                return arg

        config = rabbit_config

        container = container_factory(Service, config)
        container.start()

    @pytest.fixture(autouse=True)
    def client_container(
        self, container_factory, rabbit_config, toxic_rpc_proxy
    ):

        class Service(object):
            name = "client"

            server_rpc = RpcProxy("server")

            @dummy
            def echo(self, arg):
                return self.server_rpc.echo(arg)

        container = container_factory(Service, rabbit_config)
        container.start()
        return container

    @pytest.yield_fixture(autouse=True)
    def retry(self, request):
        retry = False
        if "publish_retry" in request.keywords:
            retry = True

        with patch.object(RpcProxy.publisher_cls, 'retry', new=retry):
            yield

    @pytest.yield_fixture(params=[True, False])
    def use_confirms(self, request):
        with patch.object(
            RpcProxy.publisher_cls, 'use_confirms', new=request.param
        ):
            yield request.param

    @pytest.yield_fixture(autouse=True)
    def toxic_rpc_proxy(self, toxiproxy):
        with patch.object(RpcProxy, 'amqp_uri', new=toxiproxy.uri):
            yield

    @pytest.mark.usefixtures('use_confirms')
    def test_normal(self, client_container):

        with entrypoint_hook(client_container, 'echo') as echo:
            assert echo(1) == 1
            assert echo(2) == 2

    @pytest.mark.usefixtures('use_confirms')
    def test_down(self, client_container, toxiproxy):
        with toxiproxy.disabled():

            with pytest.raises(OperationalError) as exc_info:
                with entrypoint_hook(client_container, 'echo') as echo:
                    echo(1)
            assert "ECONNREFUSED" in str(exc_info.value)

    @pytest.mark.usefixtures('use_confirms')
    def test_timeout(self, client_container, toxiproxy):
        with toxiproxy.timeout():

            with pytest.raises(OperationalError) as exc_info:
                with entrypoint_hook(client_container, 'echo') as echo:
                    echo(1)
            assert "Socket closed" in str(exc_info.value)

    def test_reuse_when_down(self, client_container, toxiproxy):
        """ Verify we detect stale connections.

        Publish confirms are required for this functionality. Without confirms
        the later messages are silently lost and the test hangs waiting for a
        response.
        """
        with entrypoint_hook(client_container, 'echo') as echo:
            assert echo(1) == 1

        with toxiproxy.disabled():

            with pytest.raises(IOError) as exc_info:
                with entrypoint_hook(client_container, 'echo') as echo:
                    echo(2)
            assert (
                # expect the write to raise a BrokenPipe or, if it succeeds,
                # the socket to be closed on the subsequent confirmation read
                "Broken pipe" in str(exc_info.value) or
                "Socket closed" in str(exc_info.value)
            )

    def test_reuse_when_recovered(self, client_container, toxiproxy):
        """ Verify we detect and recover from stale connections.

        Publish confirms are required for this functionality. Without confirms
        the later messages are silently lost and the test hangs waiting for a
        response.
        """
        with entrypoint_hook(client_container, 'echo') as echo:
            assert echo(1) == 1

        with toxiproxy.disabled():

            with pytest.raises(IOError) as exc_info:
                with entrypoint_hook(client_container, 'echo') as echo:
                    echo(2)
            assert (
                # expect the write to raise a BrokenPipe or, if it succeeds,
                # the socket to be closed on the subsequent confirmation read
                "Broken pipe" in str(exc_info.value) or
                "Socket closed" in str(exc_info.value)
            )

        with entrypoint_hook(client_container, 'echo') as echo:
            assert echo(3) == 3

    @pytest.mark.publish_retry
    def test_with_retry_policy(self, client_container, toxiproxy):
        """ Verify we automatically recover from stale connections.

        Publish confirms are required for this functionality. Without confirms
        the later messages are silently lost and the test hangs waiting for a
        response.
        """
        with entrypoint_hook(client_container, 'echo') as echo:
            assert echo(1) == 1

        toxiproxy.disable()

        def enable_after_retry(args, kwargs, res, exc_info):
            toxiproxy.enable()
            return True

        # call 2 succeeds (after reconnecting via retry policy)
        with wait_for_call(Connection, 'connect', callback=enable_after_retry):
            with entrypoint_hook(client_container, 'echo') as echo:
                assert echo(2) == 2


@skip_if_no_toxiproxy
class TestResponderDisconnections(object):
    """ Test and demonstrate behaviour under poor network conditions.

    Publisher confirms must be enabled for some of these tests to pass. Without
    confirms, previously used but now dead connections will accept writes
    without raising. These tests are skipped in this scenario.

    Note that publisher confirms do not protect against sockets that remain
    open but do not deliver messages (i.e. `toxiproxy.set_timeout(0)`).
    This can only be mitigated with AMQP heartbeats (not yet supported)
    """

    @pytest.yield_fixture(autouse=True)
    def toxic_responder(self, toxiproxy):
        def replacement_constructor(amqp_uri, *args, **kwargs):
            return Responder(toxiproxy.uri, *args, **kwargs)
        with patch('nameko.rpc.Responder', wraps=replacement_constructor):
            yield

    @pytest.yield_fixture(params=[True, False])
    def use_confirms(self, request):
        value = request.param
        with patch.object(Responder.publisher_cls, 'use_confirms', new=value):
            yield value

    @pytest.yield_fixture(autouse=True)
    def retry(self, request):
        value = False
        if "publish_retry" in request.keywords:
            value = True

        with patch.object(Responder.publisher_cls, 'retry', new=value):
            yield

    @pytest.fixture
    def service_cls(self):
        class Service(object):
            name = "service"

            @rpc
            def echo(self, arg):
                return arg

        return Service

    @pytest.fixture(autouse=True)
    def container(self, container_factory, service_cls, rabbit_config):

        config = rabbit_config

        container = container_factory(service_cls, config)
        container.start()
        return container

    @pytest.yield_fixture
    def service_rpc(self, rabbit_config):

        gts = []

        def kill_greenthreads():
            for gt in gts:
                try:
                    gt.kill()
                except GreenletExit:  # pragma: no cover
                    pass
            del gts[:]

        class ThreadSafeRpcProxy(object):
            """ The ServiceRpcProxy is not thread-safe, so we can't create it
            in a fixture and use it directly from a new greenlet in a test
            (which we need to, because it hangs in many of these tests).

            Instead we fake the interface and make the underlying proxy call
            in a dedicated thread, ensuring it's never shared.
            """

            def __getattr__(self, name):
                def method(*args, **kwargs):
                    def call():
                        with ServiceRpcProxy(
                            "service", rabbit_config
                        ) as proxy:
                            return getattr(proxy, name)(*args, **kwargs)

                    gt = eventlet.spawn(call)
                    gts.append(gt)
                    return gt.wait()
                return method

            def abort(self):
                kill_greenthreads()

        proxy = ThreadSafeRpcProxy()
        yield proxy
        kill_greenthreads()

    @pytest.mark.usefixtures('use_confirms')
    def test_normal(self, service_rpc):
        assert service_rpc.echo(1) == 1
        assert service_rpc.echo(2) == 2

    @pytest.mark.usefixtures('use_confirms')
    def test_down(self, container, service_rpc, toxiproxy):
        with toxiproxy.disabled():

            eventlet.spawn_n(service_rpc.echo, 1)

            # the container will raise if the responder cannot reply
            with pytest.raises(OperationalError) as exc_info:
                container.wait()
            assert "ECONNREFUSED" in str(exc_info.value)

            service_rpc.abort()

    @pytest.mark.usefixtures('use_confirms')
    def test_timeout(self, container, service_rpc, toxiproxy):
        with toxiproxy.timeout():

            eventlet.spawn_n(service_rpc.echo, 1)

            # the container will raise if the responder cannot reply
            with pytest.raises(OperationalError) as exc_info:
                container.wait()
            assert "Socket closed" in str(exc_info.value)

            service_rpc.abort()

    def test_reuse_when_down(self, container, service_rpc, toxiproxy):
        """ Verify we detect stale connections.

        Publish confirms are required for this functionality. Without confirms
        the later messages are silently lost and the test hangs waiting for a
        response.
        """
        assert service_rpc.echo(1) == 1

        with toxiproxy.disabled():

            eventlet.spawn_n(service_rpc.echo, 2)

            # the container will raise if the responder cannot reply
            with pytest.raises(IOError) as exc_info:
                container.wait()
            assert (
                # expect the write to raise a BrokenPipe or, if it succeeds,
                # the socket to be closed on the subsequent confirmation read
                "Broken pipe" in str(exc_info.value) or
                "Socket closed" in str(exc_info.value)
            )

            service_rpc.abort()

    def test_reuse_when_recovered(
        self, container, service_rpc, toxiproxy, container_factory,
        rabbit_config, service_cls
    ):
        """ Verify we detect and recover from stale connections.

        Publish confirms are required for this functionality. Without confirms
        the later messages are silently lost and the test hangs waiting for a
        response.
        """
        assert service_rpc.echo(1) == 1

        with toxiproxy.disabled():

            eventlet.spawn_n(service_rpc.echo, 2)

            # the container will raise if the responder cannot reply
            with pytest.raises(IOError) as exc_info:
                container.wait()
            assert (
                # expect the write to raise a BrokenPipe or, if it succeeds,
                # the socket to be closed on the subsequent confirmation read
                "Broken pipe" in str(exc_info.value) or
                "Socket closed" in str(exc_info.value)
            )

        # create new container
        replacement_container = container_factory(service_cls, rabbit_config)
        replacement_container.start()

        assert service_rpc.echo(3) == 3

    @pytest.mark.publish_retry
    def test_with_retry_policy(self, service_rpc, toxiproxy):
        """ Verify we automatically recover from stale connections.

        Publish confirms are required for this functionality. Without confirms
        the later messages are silently lost and the test hangs waiting for a
        response.
        """
        assert service_rpc.echo(1) == 1

        toxiproxy.disable()

        def enable_after_retry(args, kwargs, res, exc_info):
            toxiproxy.enable()
            return True

        # call 2 succeeds (after reconnecting via retry policy)
        with wait_for_call(Connection, 'connect', callback=enable_after_retry):
            assert service_rpc.echo(2) == 2


class TestRpcConsumerConfigurability(object):

    def test_heartbeat(self, mock_container):
        mock_container.config = {'AMQP_URI': 'memory://localhost'}
        mock_container.shared_extensions = {}
        mock_container.service_name = "service"

        value = 999

        rpc_consumer = RpcConsumer(heartbeat=value).bind(mock_container)
        rpc_consumer.setup()

        assert rpc_consumer.consumer.connection.heartbeat == value

    def test_prefetch_count(self, mock_container):
        mock_container.config = {'AMQP_URI': 'memory://localhost'}
        mock_container.shared_extensions = {}
        mock_container.service_name = "service"

        value = 999

        rpc_consumer = RpcConsumer(prefetch_count=value).bind(mock_container)
        rpc_consumer.setup()

        assert rpc_consumer.consumer.prefetch_count == value

    def test_accept(self, mock_container):
        mock_container.config = {'AMQP_URI': 'memory://localhost'}
        mock_container.shared_extensions = {}
        mock_container.service_name = "service"

        value = ['yaml', 'json']

        rpc_consumer = RpcConsumer(accept=value).bind(mock_container)
        rpc_consumer.setup()

        assert rpc_consumer.consumer.accept == value


class TestReplyListenerConfigurability(object):

    def test_heartbeat(self, mock_container):
        mock_container.config = {'AMQP_URI': 'memory://localhost'}
        mock_container.shared_extensions = {}
        mock_container.service_name = "service"

        value = 999

        reply_listener = ReplyListener(heartbeat=value).bind(mock_container)
        reply_listener.setup()

        assert reply_listener.consumer.connection.heartbeat == value

    def test_prefetch_count(self, mock_container):
        mock_container.config = {'AMQP_URI': 'memory://localhost'}
        mock_container.shared_extensions = {}
        mock_container.service_name = "service"

        value = 999

        reply_listener = ReplyListener(
            prefetch_count=value
        ).bind(mock_container)
        reply_listener.setup()

        assert reply_listener.consumer.prefetch_count == value

    def test_accept(self, mock_container):
        mock_container.config = {'AMQP_URI': 'memory://localhost'}
        mock_container.shared_extensions = {}
        mock_container.service_name = "service"

        value = ['yaml', 'json']

        reply_listener = ReplyListener(accept=value).bind(mock_container)
        reply_listener.setup()

        assert reply_listener.consumer.accept == value


class TestConfigurability(object):
    """
    Test and demonstrate configuration options for the RpcProxy
    """

    @pytest.yield_fixture
    def get_producer(self):
        with patch('nameko.amqp.publish.get_producer') as get_producer:
            yield get_producer

    @pytest.fixture
    def producer(self, get_producer):
        producer = get_producer().__enter__.return_value
        # make sure we don't raise UndeliverableMessage if mandatory is True
        producer.channel.returned_messages.get_nowait.side_effect = queue.Empty
        return producer

    @pytest.mark.parametrize("parameter", [
        # delivery options
        'delivery_mode', 'priority', 'expiration',
        # message options
        'serializer', 'compression',
        # retry policy
        'retry', 'retry_policy',
        # other arbitrary publish kwargs
        'user_id', 'bogus_param'
    ])
    def test_regular_parameters(
        self, parameter, mock_container, producer
    ):
        """ Verify that most parameters can be specified at RpcProxy
        instantiation time.
        """
        mock_container.config = {'AMQP_URI': 'memory://localhost'}
        mock_container.shared_extensions = {}
        mock_container.service_name = "service"

        worker_ctx = Mock()
        worker_ctx.container = mock_container
        worker_ctx.context_data = {}

        value = Mock()

        rpc_proxy = RpcProxy(
            "service-name", **{parameter: value}
        ).bind(mock_container, "service_rpc")

        rpc_proxy.reply_listener.setup()
        rpc_proxy.setup()

        service_rpc = rpc_proxy.get_dependency(worker_ctx)

        service_rpc.method.call_async()
        assert producer.publish.call_args[1][parameter] == value

    @pytest.mark.usefixtures('predictable_call_ids')
    def test_headers(self, mock_container, producer):
        """ Headers can be provided at instantiation time, and are merged with
        Nameko headers.
        """
        mock_container.config = {'AMQP_URI': 'memory://localhost'}
        mock_container.shared_extensions = {}
        mock_container.service_name = "service-name"

        # use a real worker context so nameko headers are generated
        service = Mock()
        entrypoint = Mock(method_name="method")
        worker_ctx = WorkerContext(
            mock_container, service, entrypoint, data={'context': 'data'}
        )

        nameko_headers = {
            'nameko.context': 'data',
            'nameko.call_id_stack': ['service-name.method.0'],
        }

        value = {'foo': Mock()}

        rpc_proxy = RpcProxy(
            "service-name", **{'headers': value}
        ).bind(mock_container, "service_rpc")

        rpc_proxy.reply_listener.setup()
        rpc_proxy.setup()

        service_rpc = rpc_proxy.get_dependency(worker_ctx)

        def merge_dicts(base, *updates):
            merged = base.copy()
            [merged.update(update) for update in updates]
            return merged

        service_rpc.method.call_async()
        assert producer.publish.call_args[1]['headers'] == merge_dicts(
            nameko_headers, value
        )

    @patch('nameko.rpc.uuid')
    def test_restricted_parameters(self, patch_uuid, mock_container, producer):
        """ Verify that providing routing parameters at instantiation
        time has no effect.
        """
        mock_container.config = {'AMQP_URI': 'memory://localhost'}
        mock_container.shared_extensions = {}
        mock_container.service_name = "service"

        worker_ctx = Mock()
        worker_ctx.container = mock_container
        worker_ctx.context_data = {}

        patch_uuid.uuid4.side_effect = ["uuid1", "uuid2"]

        restricted_params = (
            'exchange', 'routing_key', 'mandatory',
            'correlation_id', 'reply_to'
        )

        rpc_proxy = RpcProxy(
            "service", **{param: Mock() for param in restricted_params}
        ).bind(mock_container, "service_rpc")

        rpc_proxy.reply_listener.setup()
        rpc_proxy.setup()

        service_rpc = rpc_proxy.get_dependency(worker_ctx)

        service_rpc.method.call_async()
        publish_params = producer.publish.call_args[1]

        assert publish_params['exchange'].name == "nameko-rpc"
        assert publish_params['routing_key'] == 'service.method'
        assert publish_params['mandatory'] is True
        assert publish_params['reply_to'] == "uuid1"
        assert publish_params['correlation_id'] == "uuid2"


def test_prefetch_throughput(container_factory, rabbit_config):
    """Make sure even max_workers=1 can consumer faster than 1 msg/second

    Regression test for https://github.com/nameko/nameko/issues/417
    """

    class Service(object):
        name = "service"

        @rpc
        def method(self):
            pass

    rabbit_config[MAX_WORKERS_CONFIG_KEY] = 1
    container = container_factory(Service, rabbit_config)
    container.start()

    replies = []
    with ServiceRpcProxy("service", rabbit_config) as proxy:
        for _ in range(5):
            replies.append(proxy.method.call_async())

        with eventlet.Timeout(1):
            [reply.result() for reply in replies]


def test_stop_with_active_worker(container_factory, rabbit_config, queue_info):
    """ Test behaviour when we stop a container with an active worker.

    Expect the consumer to stop and the message be requeued, but the container
    to continue running the active worker until it completes.

    This is not desirable behaviour but it is consistent with the old
    implementation. It would be better to stop the consumer but keep the
    channel alive until the worker has completed and the message can be
    ack'd, but we can't do that with kombu or without per-entrypoint worker
    pools.
    """

    block = Event()

    class Service(object):
        name = "service"

        @rpc
        def method(self):
            block.wait()

    container = container_factory(Service, rabbit_config)
    container.start()

    with ServiceRpcProxy("service", rabbit_config) as service_rpc:
        service_rpc.method.call_async()

    gt = eventlet.spawn(container.stop)

    @retry
    def consumer_removed():
        info = queue_info('rpc-service')
        assert info.consumer_count == 0
        assert info.message_count == 1

    consumer_removed()

    assert not gt.dead
    block.send(True)

    gt.wait()
    assert gt.dead


class TestSSL(object):

    @pytest.fixture(params=[True, False])
    def rabbit_ssl_config(self, request, rabbit_ssl_config):
        verify_certs = request.param
        if verify_certs is False:
            # remove certificate paths from config
            rabbit_ssl_config['AMQP_SSL'] = True
        return rabbit_ssl_config

    def test_rpc_entrypoint_over_ssl(
        self, container_factory, rabbit_ssl_config, rabbit_config
    ):
        class Service(object):
            name = "service"

            @rpc
            def echo(self, *args, **kwargs):
                return args, kwargs

        container = container_factory(Service, rabbit_ssl_config)
        container.start()

        with ServiceRpcProxy("service", rabbit_config) as proxy:
            assert proxy.echo("a", "b", foo="bar") == [
                ['a', 'b'], {'foo': 'bar'}
            ]

    def test_rpc_proxy_over_ssl(
        self, container_factory, rabbit_ssl_config, rabbit_config
    ):
        class Service(object):
            name = "service"

            delegate_rpc = RpcProxy('delegate')

            @dummy
            def echo(self, *args, **kwargs):
                return self.delegate_rpc.echo(*args, **kwargs)

        class Delegate(object):
            name = "delegate"

            @rpc
            def echo(self, *args, **kwargs):
                return args, kwargs

        container = container_factory(Service, rabbit_ssl_config)
        container.start()

        delegate = container_factory(Delegate, rabbit_config)
        delegate.start()

        with entrypoint_hook(container, 'echo') as echo:
            assert echo("a", "b", foo="bar") == [
                ['a', 'b'], {'foo': 'bar'}
            ]
