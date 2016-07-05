import socket
from distutils import spawn

import eventlet
import pytest
from kombu import Exchange, Queue
from kombu.connection import Connection
from mock import Mock, call, patch

from nameko.constants import DEFAULT_RETRY_POLICY
from nameko.containers import (
    NAMEKO_CONTEXT_KEYS, WorkerContext, WorkerContextBase)
from nameko.exceptions import ContainerBeingKilled
from nameko.messaging import (
    Consumer, HeaderDecoder, HeaderEncoder, Publisher, consume)
from nameko.testing.services import dummy, entrypoint_hook, entrypoint_waiter
from nameko.testing.utils import (
    ANY_PARTIAL, DummyProvider, wait_for_call, worker_context_factory)
from nameko.testing.waiting import wait_for_call as patch_wait

foobar_ex = Exchange('foobar_ex', durable=False)
foobar_queue = Queue('foobar_queue', exchange=foobar_ex, durable=False)

CONSUME_TIMEOUT = 1


class CustomWorkerContext(WorkerContextBase):
    context_keys = NAMEKO_CONTEXT_KEYS + ('customheader',)


@pytest.yield_fixture
def maybe_declare():
    with patch('nameko.messaging.maybe_declare', autospec=True) as patched:
        yield patched


def test_consume_provider(mock_container):

    container = mock_container
    container.shared_extensions = {}
    container.worker_ctx_cls = WorkerContext
    container.service_name = "service"

    worker_ctx = WorkerContext(container, None, DummyProvider())

    spawn_worker = container.spawn_worker
    spawn_worker.return_value = worker_ctx

    queue_consumer = Mock()

    consume_provider = Consumer(
        queue=foobar_queue, requeue_on_error=False).bind(container, "consume")
    consume_provider.queue_consumer = queue_consumer

    message = Mock(headers={})

    # test lifecycle
    consume_provider.setup()
    queue_consumer.register_provider.assert_called_once_with(
        consume_provider)

    consume_provider.stop()
    queue_consumer.unregister_provider.assert_called_once_with(
        consume_provider)

    # test handling successful call
    queue_consumer.reset_mock()
    consume_provider.handle_message("body", message)
    handle_result = spawn_worker.call_args[1]['handle_result']
    handle_result(worker_ctx, 'result')
    queue_consumer.ack_message.assert_called_once_with(message)

    # test handling failed call without requeue
    queue_consumer.reset_mock()
    consume_provider.requeue_on_error = False
    consume_provider.handle_message("body", message)
    handle_result = spawn_worker.call_args[1]['handle_result']
    handle_result(worker_ctx, None, (Exception, Exception('Error'), "tb"))
    queue_consumer.ack_message.assert_called_once_with(message)

    # test handling failed call with requeue
    queue_consumer.reset_mock()
    consume_provider.requeue_on_error = True
    consume_provider.handle_message("body", message)
    handle_result = spawn_worker.call_args[1]['handle_result']
    handle_result(worker_ctx, None, (Exception, Exception('Error'), "tb"))
    assert not queue_consumer.ack_message.called
    queue_consumer.requeue_message.assert_called_once_with(message)

    # test requeueing on ContainerBeingKilled (even without requeue_on_error)
    queue_consumer.reset_mock()
    consume_provider.requeue_on_error = False
    spawn_worker.side_effect = ContainerBeingKilled()
    consume_provider.handle_message("body", message)
    assert not queue_consumer.ack_message.called
    queue_consumer.requeue_message.assert_called_once_with(message)


@pytest.mark.usefixtures("predictable_call_ids")
def test_publish_to_exchange(
    maybe_declare, mock_connection, mock_producer, mock_container
):
    container = mock_container
    container.service_name = "srcservice"

    service = Mock()
    worker_ctx = WorkerContext(container, service, DummyProvider("publish"))

    publisher = Publisher(exchange=foobar_ex).bind(container, "publish")

    # test declarations
    publisher.setup()
    maybe_declare.assert_called_once_with(foobar_ex, mock_connection)

    # test publish
    msg = "msg"
    service.publish = publisher.get_dependency(worker_ctx)
    service.publish(msg, publish_kwarg="value")
    headers = {
        'nameko.call_id_stack': ['srcservice.publish.0']
    }
    mock_producer.publish.assert_called_once_with(
        msg, headers=headers, exchange=foobar_ex, retry=True,
        serializer=container.serializer,
        retry_policy=DEFAULT_RETRY_POLICY, publish_kwarg="value")


@pytest.mark.usefixtures("predictable_call_ids")
def test_publish_to_queue(
    maybe_declare, mock_producer, mock_connection, mock_container
):
    container = mock_container
    container.shared_extensions = {}
    container.service_name = "srcservice"

    ctx_data = {'language': 'en'}
    service = Mock()
    worker_ctx = WorkerContext(
        container, service, DummyProvider("publish"), data=ctx_data)

    publisher = Publisher(queue=foobar_queue).bind(container, "publish")

    # test declarations
    publisher.setup()
    maybe_declare.assert_called_once_with(foobar_queue, mock_connection)

    # test publish
    msg = "msg"
    headers = {
        'nameko.language': 'en',
        'nameko.call_id_stack': ['srcservice.publish.0'],
    }
    service.publish = publisher.get_dependency(worker_ctx)
    service.publish(msg, publish_kwarg="value")
    mock_producer.publish.assert_called_once_with(
        msg, headers=headers, exchange=foobar_ex, retry=True,
        serializer=container.serializer,
        retry_policy=DEFAULT_RETRY_POLICY, publish_kwarg="value")


@pytest.mark.usefixtures("predictable_call_ids")
def test_publish_custom_headers(
    mock_container, maybe_declare, mock_producer, mock_connection
):

    container = mock_container
    container.service_name = "srcservice"

    ctx_data = {'language': 'en', 'customheader': 'customvalue'}
    service = Mock()
    worker_ctx = CustomWorkerContext(container, service,
                                     DummyProvider('method'), data=ctx_data)

    publisher = Publisher(queue=foobar_queue).bind(container, "publish")

    # test declarations
    publisher.setup()
    maybe_declare.assert_called_once_with(foobar_queue, mock_connection)

    # test publish
    msg = "msg"
    headers = {'nameko.language': 'en',
               'nameko.customheader': 'customvalue',
               'nameko.call_id_stack': ['srcservice.method.0']}
    service.publish = publisher.get_dependency(worker_ctx)
    service.publish(msg, publish_kwarg="value")
    mock_producer.publish.assert_called_once_with(
        msg, headers=headers, exchange=foobar_ex, retry=True,
        serializer=container.serializer,
        retry_policy=DEFAULT_RETRY_POLICY, publish_kwarg="value")


def test_header_encoder(empty_config):

    context_data = {
        'foo': 'FOO',
        'bar': 'BAR',
        'baz': 'BAZ',
        # unserialisable, shouldn't be included in the processed headers
        'none': None,
    }

    encoder = HeaderEncoder()
    with patch.object(encoder, 'header_prefix', new="testprefix"):

        worker_ctx_cls = worker_context_factory('foo', 'bar', 'xxx')
        worker_ctx = worker_ctx_cls(data=context_data)
        worker_ctx.call_id_stack = ['x']

        res = encoder.get_message_headers(worker_ctx)
        assert res == {'testprefix.foo': 'FOO', 'testprefix.bar': 'BAR',
                       'testprefix.call_id_stack': ['x']}


def test_header_decoder():

    headers = {
        'testprefix.foo': 'FOO',
        'testprefix.bar': 'BAR',
        'testprefix.baz': 'BAZ',
        'bogusprefix.foo': 'XXX',
        'testprefix.call_id_stack': ['a', 'b', 'c'],
    }

    decoder = HeaderDecoder()
    with patch.object(decoder, 'header_prefix', new="testprefix"):

        worker_ctx_cls = worker_context_factory("foo", "bar", "call_id_stack")
        message = Mock(headers=headers)

        res = decoder.unpack_message_headers(worker_ctx_cls, message)
        assert res == {
            'foo': 'FOO',
            'bar': 'BAR',
            'call_id_stack': ['a', 'b', 'c'],
        }


# =============================================================================
# INTEGRATION TESTS
# =============================================================================

@pytest.mark.usefixtures("predictable_call_ids")
def test_publish_to_rabbit(rabbit_manager, rabbit_config, mock_container):

    vhost = rabbit_config['vhost']

    container = mock_container
    container.service_name = "service"
    container.config = rabbit_config
    container.spawn_managed_thread = eventlet.spawn

    ctx_data = {'language': 'en', 'customheader': 'customvalue'}
    service = Mock()
    worker_ctx = CustomWorkerContext(container, service,
                                     DummyProvider('method'), data=ctx_data)

    publisher = Publisher(
        exchange=foobar_ex, queue=foobar_queue).bind(container, "publish")

    # test queue, exchange and binding created in rabbit
    publisher.setup()
    publisher.start()

    exchanges = rabbit_manager.get_exchanges(vhost)
    queues = rabbit_manager.get_queues(vhost)
    bindings = rabbit_manager.get_queue_bindings(vhost, foobar_queue.name)

    assert "foobar_ex" in [exchange['name'] for exchange in exchanges]
    assert "foobar_queue" in [queue['name'] for queue in queues]
    assert "foobar_ex" in [binding['source'] for binding in bindings]

    # test message published to queue
    service.publish = publisher.get_dependency(worker_ctx)
    service.publish("msg")
    messages = rabbit_manager.get_messages(vhost, foobar_queue.name)
    assert ['"msg"'] == [msg['payload'] for msg in messages]

    # test message headers
    assert messages[0]['properties']['headers'] == {
        'nameko.language': 'en',
        'nameko.customheader': 'customvalue',
        'nameko.call_id_stack': ['service.method.0'],
    }


@pytest.mark.usefixtures("predictable_call_ids")
def test_unserialisable_headers(rabbit_manager, rabbit_config, mock_container):

    vhost = rabbit_config['vhost']

    container = mock_container
    container.service_name = "service"
    container.config = rabbit_config
    container.spawn_managed_thread = eventlet.spawn

    ctx_data = {'language': 'en', 'customheader': None}
    service = Mock()
    worker_ctx = CustomWorkerContext(container, service,
                                     DummyProvider('method'), data=ctx_data)

    publisher = Publisher(
        exchange=foobar_ex, queue=foobar_queue).bind(container, "publish")

    publisher.setup()
    publisher.start()

    service.publish = publisher.get_dependency(worker_ctx)
    service.publish("msg")
    messages = rabbit_manager.get_messages(vhost, foobar_queue.name)

    assert messages[0]['properties']['headers'] == {
        'nameko.language': 'en',
        'nameko.call_id_stack': ['service.method.0'],
        # no `customheader`
    }


def test_consume_from_rabbit(rabbit_manager, rabbit_config, mock_container):

    vhost = rabbit_config['vhost']

    container = mock_container
    container.shared_extensions = {}
    container.worker_ctx_cls = CustomWorkerContext
    container.service_name = "service"
    container.config = rabbit_config
    container.max_workers = 10

    content_type = 'application/data'
    container.accept = [content_type]

    def spawn_thread(method, protected):
        return eventlet.spawn(method)
    container.spawn_managed_thread = spawn_thread

    worker_ctx = CustomWorkerContext(container, None, DummyProvider())

    consumer = Consumer(
        queue=foobar_queue, requeue_on_error=False).bind(container, "publish")

    # prepare and start extensions
    consumer.setup()
    consumer.queue_consumer.setup()
    consumer.start()
    consumer.queue_consumer.start()

    # test queue, exchange and binding created in rabbit
    exchanges = rabbit_manager.get_exchanges(vhost)
    queues = rabbit_manager.get_queues(vhost)
    bindings = rabbit_manager.get_queue_bindings(vhost, foobar_queue.name)

    assert "foobar_ex" in [exchange['name'] for exchange in exchanges]
    assert "foobar_queue" in [queue['name'] for queue in queues]
    assert "foobar_ex" in [binding['source'] for binding in bindings]

    # test message consumed from queue
    container.spawn_worker.return_value = worker_ctx

    headers = {'nameko.language': 'en', 'nameko.customheader': 'customvalue'}

    rabbit_manager.publish(
        vhost, foobar_ex.name, '', 'msg',
        properties=dict(headers=headers, content_type=content_type))

    ctx_data = {
        'language': 'en',
        'customheader': 'customvalue',
    }
    with wait_for_call(CONSUME_TIMEOUT, container.spawn_worker) as method:
        method.assert_called_once_with(consumer, ('msg',), {},
                                       context_data=ctx_data,
                                       handle_result=ANY_PARTIAL)
        handle_result = method.call_args[1]['handle_result']

    # ack message
    handle_result(worker_ctx, 'result')

    # stop will hang if the consumer hasn't acked or requeued messages
    with eventlet.timeout.Timeout(CONSUME_TIMEOUT):
        consumer.stop()

    consumer.queue_consumer.kill()


@pytest.mark.skipif(
    spawn.find_executable('toxiproxy-server') is None,
    reason="toxiproxy not installed"
)
class TestPublisherDisconnections(object):
    """ Test and demonstrate behaviour under poor network conditions.

    Publisher confirms must be enabled for some of these tests to pass. Without
    confirms, previously used but now dead connections will accept writes
    without raising. These tests are skipped in this scenario.

    Note that publisher confirms do not protect against sockets that remain
    open but do not deliver messages (i.e. `toxiproxy.timeout(0)`).
    This can only be mitigated with AMQP heartbeats (not yet supported)
    """

    @pytest.fixture
    def tracker(self):
        return Mock()

    @pytest.yield_fixture(autouse=True)
    def toxic_publisher(self, toxiproxy):
        with patch.object(Publisher, 'amqp_uri', new=toxiproxy.uri):
            yield

    @pytest.yield_fixture(autouse=True, params=[True, False])
    def use_confirms(self, request):
        with patch.object(Publisher, 'use_confirms', new=request.param):
            yield request.param

    @pytest.yield_fixture
    def publisher_container(
        self, request, container_factory, tracker, rabbit_config
    ):
        retry = False
        if "enable_retry" in request.keywords:
            retry = True

        class Service(object):
            name = "publish"

            publish = Publisher()

            @dummy
            def send(self, payload):
                tracker("send", payload)
                self.publish(payload, routing_key="test_queue", retry=retry)

        container = container_factory(Service, rabbit_config)
        container.start()
        yield container

    @pytest.yield_fixture
    def consumer_container(
        self, container_factory, tracker, rabbit_config
    ):
        class Service(object):
            name = "consume"

            @consume(Queue("test_queue"))
            def recv(self, payload):
                tracker("recv", payload)

        config = rabbit_config

        container = container_factory(Service, config)
        container.start()
        yield container

    def test_normal(self, publisher_container, consumer_container, tracker):

        # call 1 succeeds
        payload1 = "payload1"
        with entrypoint_waiter(consumer_container, 'recv'):
            with entrypoint_hook(publisher_container, 'send') as send:
                send(payload1)

        assert tracker.call_args_list == [
            call("send", payload1),
            call("recv", payload1),
        ]

        # call 2 succeeds
        payload2 = "payload2"
        with entrypoint_waiter(consumer_container, 'recv'):
            with entrypoint_hook(publisher_container, 'send') as send:
                send(payload2)

        assert tracker.call_args_list == [
            call("send", payload1),
            call("recv", payload1),
            call("send", payload2),
            call("recv", payload2),
        ]

    def test_down(
        self, publisher_container, consumer_container, tracker, toxiproxy
    ):
        toxiproxy.disable()

        payload1 = "payload1"
        with pytest.raises(socket.error) as exc_info:
            with entrypoint_hook(publisher_container, 'send') as send:
                send(payload1)
        assert "ECONNREFUSED" in str(exc_info.value)

        assert tracker.call_args_list == [
            call("send", payload1),
        ]

    def test_timeout(
        self, publisher_container, consumer_container, tracker, toxiproxy
    ):
        toxiproxy.timeout(500)

        payload1 = "payload1"
        with pytest.raises(IOError) as exc_info:  # socket closed
            with entrypoint_hook(publisher_container, 'send') as send:
                send(payload1)
        assert "Socket closed" in str(exc_info.value)

        assert tracker.call_args_list == [
            call("send", payload1),
        ]

    def test_reuse_when_down(
        self, publisher_container, consumer_container, tracker, toxiproxy,
        use_confirms
    ):
        if not use_confirms:
            pytest.skip(
                "unconfirmed messages will be lost after disconnection"
            )

        # call 1 succeeds
        payload1 = "payload1"
        with entrypoint_waiter(consumer_container, 'recv'):
            with entrypoint_hook(publisher_container, 'send') as send:
                send(payload1)

        assert tracker.call_args_list == [
            call("send", payload1),
            call("recv", payload1),
        ]

        toxiproxy.disable()

        # call 2 fails
        payload2 = "payload2"
        with pytest.raises(IOError) as exc_info:
            with entrypoint_hook(publisher_container, 'send') as send:
                send(payload2)
        assert (
            # expect the write to raise a BrokenPipe or, if it succeeds,
            # the socket to be closed on the subsequent confirmation read
            "Broken pipe" in str(exc_info.value) or
            "Socket closed" in str(exc_info.value)
        )

        assert tracker.call_args_list == [
            call("send", payload1),
            call("recv", payload1),
            call("send", payload2),
        ]

    def test_reuse_when_recovered(
        self, publisher_container, consumer_container, tracker, toxiproxy,
        use_confirms
    ):
        if not use_confirms:
            pytest.skip(
                "unconfirmed messages will be lost after disconnection"
            )

        # call 1 succeeds
        payload1 = "payload1"
        with entrypoint_waiter(consumer_container, 'recv'):
            with entrypoint_hook(publisher_container, 'send') as send:
                send(payload1)

        assert tracker.call_args_list == [
            call("send", payload1),
            call("recv", payload1),
        ]

        toxiproxy.disable()

        # call 2 fails
        payload2 = "payload2"
        with pytest.raises(IOError) as exc_info:
            with entrypoint_hook(publisher_container, 'send') as send:
                send(payload2)
        assert (
            # expect the write to raise a BrokenPipe or, if it succeeds,
            # the socket to be closed on the subsequent confirmation read
            "Broken pipe" in str(exc_info.value) or
            "Socket closed" in str(exc_info.value)
        )

        assert tracker.call_args_list == [
            call("send", payload1),
            call("recv", payload1),
            call("send", payload2),
        ]

        toxiproxy.enable()

        # call 3 succeeds
        payload3 = "payload3"
        with entrypoint_waiter(consumer_container, 'recv'):
            with entrypoint_hook(publisher_container, 'send') as send:
                send(payload3)

        assert tracker.call_args_list == [
            call("send", payload1),
            call("recv", payload1),
            call("send", payload2),
            call("send", payload3),
            call("recv", payload3),
        ]

    @pytest.mark.enable_retry
    def test_with_retry_policy(
        self, publisher_container, consumer_container, tracker, toxiproxy,
        use_confirms
    ):
        if not use_confirms:
            pytest.skip(
                "unconfirmed messages will be lost after disconnection"
            )

        # call 1 succeeds
        payload1 = "payload1"
        with entrypoint_waiter(consumer_container, 'recv'):
            with entrypoint_hook(publisher_container, 'send') as send:
                send(payload1)

        assert tracker.call_args_list == [
            call("send", payload1),
            call("recv", payload1),
        ]

        toxiproxy.disable()

        def enable_after_retry(args, kwargs, res, exc_info):
            toxiproxy.enable()
            return True

        # call 2 succeeds (after reconnecting via retry policy)
        with patch_wait(Connection, 'connect', callback=enable_after_retry):

            payload2 = "payload2"
            with entrypoint_waiter(consumer_container, 'recv'):
                with entrypoint_hook(publisher_container, 'send') as send:
                    send(payload2)

            assert tracker.call_args_list == [
                call("send", payload1),
                call("recv", payload1),
                call("send", payload2),
                call("recv", payload2),
            ]
