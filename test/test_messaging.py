import socket
from datetime import datetime

import eventlet
import pytest
from amqp.exceptions import PreconditionFailed
from kombu import Exchange, Queue
from kombu.common import maybe_declare
from kombu.connection import Connection
from kombu.compression import get_encoder
from kombu.serialization import registry
from mock import Mock, call, patch

from test import skip_if_no_toxiproxy

from nameko.amqp import get_connection, UndeliverableMessage
from nameko.containers import WorkerContext
from nameko.exceptions import ContainerBeingKilled
from nameko.messaging import (
    Consumer, HeaderDecoder, HeaderEncoder, Publisher, consume)
from nameko.testing.services import dummy, entrypoint_hook, entrypoint_waiter
from nameko.testing.utils import ANY_PARTIAL, DummyProvider, wait_for_call
from nameko.testing.waiting import wait_for_call as patch_wait

foobar_ex = Exchange('foobar_ex', durable=False)
foobar_queue = Queue('foobar_queue', exchange=foobar_ex, durable=False)

CONSUME_TIMEOUT = 1


@pytest.yield_fixture
def patch_maybe_declare():
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
    patch_maybe_declare, mock_connection, mock_producer, mock_container
):
    container = mock_container
    container.service_name = "srcservice"

    service = Mock()
    worker_ctx = WorkerContext(container, service, DummyProvider("publish"))

    publisher = Publisher(exchange=foobar_ex).bind(container, "publish")

    # test declarations
    publisher.setup()
    patch_maybe_declare.assert_called_once_with(foobar_ex, mock_connection)

    # test publish
    msg = "msg"
    service.publish = publisher.get_dependency(worker_ctx)
    service.publish(msg, publish_kwarg="value")

    headers = {
        'nameko.call_id_stack': ['srcservice.publish.0']
    }

    expected_args = ('msg',)
    expected_kwargs = {
        'publish_kwarg': "value",
        'exchange': foobar_ex,
        'headers': headers,
        'retry': publisher.retry,
        'retry_policy': publisher.retry_policy
    }
    expected_kwargs.update(publisher.delivery_options)
    expected_kwargs.update(publisher.encoding_options)

    assert mock_producer.publish.call_args_list == [
        call(*expected_args, **expected_kwargs)
    ]


@pytest.mark.usefixtures("predictable_call_ids")
def test_publish_to_queue(
    patch_maybe_declare, mock_producer, mock_connection, mock_container
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
    patch_maybe_declare.assert_called_once_with(foobar_queue, mock_connection)

    # test publish
    msg = "msg"
    headers = {
        'nameko.language': 'en',
        'nameko.call_id_stack': ['srcservice.publish.0'],
    }
    service.publish = publisher.get_dependency(worker_ctx)
    service.publish(msg, publish_kwarg="value")

    expected_args = ('msg',)
    expected_kwargs = {
        'publish_kwarg': "value",
        'exchange': foobar_ex,
        'headers': headers,
        'retry': publisher.retry,
        'retry_policy': publisher.retry_policy
    }
    expected_kwargs.update(publisher.delivery_options)
    expected_kwargs.update(publisher.encoding_options)

    assert mock_producer.publish.call_args_list == [
        call(*expected_args, **expected_kwargs)
    ]


@pytest.mark.usefixtures("predictable_call_ids")
def test_publish_custom_headers(
    mock_container, patch_maybe_declare, mock_producer, mock_connection
):

    container = mock_container
    container.service_name = "srcservice"

    ctx_data = {'language': 'en', 'customheader': 'customvalue'}
    service = Mock()
    worker_ctx = WorkerContext(
        container, service, DummyProvider('method'), data=ctx_data
    )

    publisher = Publisher(queue=foobar_queue).bind(container, "publish")

    # test declarations
    publisher.setup()
    patch_maybe_declare.assert_called_once_with(foobar_queue, mock_connection)

    # test publish
    msg = "msg"
    headers = {'nameko.language': 'en',
               'nameko.customheader': 'customvalue',
               'nameko.call_id_stack': ['srcservice.method.0']}
    service.publish = publisher.get_dependency(worker_ctx)
    service.publish(msg, publish_kwarg="value")

    expected_args = ('msg',)
    expected_kwargs = {
        'publish_kwarg': "value",
        'exchange': foobar_ex,
        'headers': headers,
        'retry': publisher.retry,
        'retry_policy': publisher.retry_policy
    }
    expected_kwargs.update(publisher.delivery_options)
    expected_kwargs.update(publisher.encoding_options)

    assert mock_producer.publish.call_args_list == [
        call(*expected_args, **expected_kwargs)
    ]


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

        worker_ctx = Mock(context_data=context_data)

        res = encoder.get_message_headers(worker_ctx)
        assert res == {
            'testprefix.foo': 'FOO',
            'testprefix.bar': 'BAR',
            'testprefix.baz': 'BAZ',
        }


def test_header_decoder():

    headers = {
        'testprefix.foo': 'FOO',
        'testprefix.bar': 'BAR',
        'testprefix.baz': 'BAZ',
        'differentprefix.foo': 'XXX',
        'testprefix.call_id_stack': ['a', 'b', 'c'],
    }

    decoder = HeaderDecoder()
    with patch.object(decoder, 'header_prefix', new="testprefix"):

        message = Mock(headers=headers)

        res = decoder.unpack_message_headers(None, message)
        assert res == {
            'foo': 'FOO',
            'bar': 'BAR',
            'baz': 'BAZ',
            'call_id_stack': ['a', 'b', 'c'],
            'differentprefix.foo': 'XXX'
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

    ctx_data = {'language': 'en', 'customheader': 'customvalue'}
    service = Mock()
    worker_ctx = WorkerContext(
        container, service, DummyProvider('method'), data=ctx_data
    )

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
    worker_ctx = WorkerContext(
        container, service, DummyProvider('method'), data=ctx_data
    )

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
    container.worker_ctx_cls = WorkerContext
    container.service_name = "service"
    container.config = rabbit_config
    container.max_workers = 10

    content_type = 'application/data'
    container.accept = [content_type]

    def spawn_managed_thread(method):
        return eventlet.spawn(method)

    container.spawn_managed_thread = spawn_managed_thread

    worker_ctx = WorkerContext(container, None, DummyProvider())

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


@skip_if_no_toxiproxy
class TestPublisherDisconnections(object):
    """ Test and demonstrate behaviour under poor network conditions.

    Publisher confirms must be enabled for some of these tests to pass. Without
    confirms, previously used but now dead connections will accept writes
    without raising. These tests are skipped in this scenario.

    Note that publisher confirms do not protect against sockets that remain
    open but do not deliver messages (i.e. `toxiproxy.set_timeout(0)`).
    This can only be mitigated with AMQP heartbeats (not yet supported)
    """

    @pytest.fixture
    def tracker(self):
        return Mock()

    @pytest.yield_fixture(autouse=True)
    def toxic_publisher(self, toxiproxy):
        with patch.object(Publisher, 'amqp_uri', new=toxiproxy.uri):
            yield

    @pytest.yield_fixture(params=[True, False])
    def use_confirms(self, request):
        with patch.object(Publisher, 'use_confirms', new=request.param):
            yield request.param

    @pytest.yield_fixture
    def publisher_container(
        self, request, container_factory, tracker, rabbit_config
    ):
        retry = False
        if "publish_retry" in request.keywords:
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

    @pytest.mark.usefixtures('use_confirms')
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

    @pytest.mark.usefixtures('use_confirms')
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

    @pytest.mark.usefixtures('use_confirms')
    def test_timeout(
        self, publisher_container, consumer_container, tracker, toxiproxy
    ):
        toxiproxy.set_timeout(500)

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
    ):
        """ Verify we detect stale connections.

        Publish confirms are required for this functionality. Without confirms
        the later messages are silently lost and the test hangs waiting for a
        response.
        """
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
        self, publisher_container, consumer_container, tracker, toxiproxy
    ):
        """ Verify we detect and recover from stale connections.

        Publish confirms are required for this functionality. Without confirms
        the later messages are silently lost and the test hangs waiting for a
        response.
        """
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

    @pytest.mark.publish_retry
    def test_with_retry_policy(
        self, publisher_container, consumer_container, tracker, toxiproxy
    ):
        """ Verify we automatically recover from stale connections.

        Publish confirms are required for this functionality. Without confirms
        the later messages are silently lost and the test hangs waiting for a
        response.
        """
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


class TestPublisherOptionPrecedence(object):

    @pytest.fixture
    def routing_key(self):
        return "routing_key"

    @pytest.fixture
    def exchange(self, amqp_uri):
        """ Make a "sniffer" queue bound to the exchange receiving messages
        """
        exchange = Exchange(name="exchange")
        with get_connection(amqp_uri) as connection:
            maybe_declare(exchange, connection)
        return exchange

    @pytest.fixture
    def queue(self, amqp_uri, exchange, routing_key):
        queue = Queue(
            name="queue", exchange=exchange, routing_key=routing_key
        )

        with get_connection(amqp_uri) as connection:
            maybe_declare(queue, connection)
        return queue

    @pytest.fixture
    def service_base(self, exchange):

        class Service(object):
            name = "service"

            publish = Publisher(exchange=exchange)

            @dummy
            def proxy(self, *args, **kwargs):
                self.publish(*args, **kwargs)

        return Service

    def test_subclass(
        self, container_factory, rabbit_config, get_message_from_queue, queue,
        service_base, routing_key, exchange
    ):

        class ExpiringPublisher(Publisher):
            delivery_options = {
                'expiration': 1
            }

        class Service(service_base):
            publish = ExpiringPublisher(exchange=exchange)

        container = container_factory(Service, rabbit_config)
        container.start()

        with entrypoint_hook(container, "proxy") as publish:
            publish("payload", routing_key=routing_key)

        message = get_message_from_queue(queue.name)
        assert message.properties['expiration'] == str(1 * 1000)

    def test_declare_time(
        self, container_factory, rabbit_config, get_message_from_queue, queue,
        service_base, routing_key, exchange
    ):

        class Service(service_base):
            publish = Publisher(exchange=exchange, expiration=1)

        container = container_factory(Service, rabbit_config)
        container.start()

        with entrypoint_hook(container, "proxy") as publish:
            publish("payload", routing_key=routing_key)

        message = get_message_from_queue(queue.name)
        assert message.properties['expiration'] == str(1 * 1000)

    def test_publish_time(
        self, container_factory, rabbit_config, get_message_from_queue, queue,
        service_base, routing_key
    ):
        container = container_factory(service_base, rabbit_config)
        container.start()

        with entrypoint_hook(container, "proxy") as publish:
            publish("payload", routing_key=routing_key, expiration=1)

        message = get_message_from_queue(queue.name)
        assert message.properties['expiration'] == str(1 * 1000)

    def test_publish_time_override(
        self, container_factory, rabbit_config, get_message_from_queue, queue,
        service_base, routing_key, exchange
    ):
        class Service(service_base):
            publish = Publisher(exchange=exchange, expiration=1)

        container = container_factory(service_base, rabbit_config)
        container.start()

        with entrypoint_hook(container, "proxy") as publish:
            publish("payload", routing_key=routing_key, expiration=2)

        message = get_message_from_queue(queue.name)
        assert message.properties['expiration'] == str(2 * 1000)


@pytest.mark.behavioural
class TestPublisherOptions(object):

    @pytest.fixture
    def routing_key(self):
        return "routing_key"

    @pytest.fixture
    def exchange(self, amqp_uri):
        """ Make a "sniffer" queue bound to the exchange receiving messages
        """
        exchange = Exchange(name="exchange")
        with get_connection(amqp_uri) as connection:
            maybe_declare(exchange, connection)
        return exchange

    @pytest.fixture
    def queue(self, amqp_uri, exchange, routing_key):
        queue = Queue(
            name="queue", exchange=exchange, routing_key=routing_key
        )

        with get_connection(amqp_uri) as connection:
            maybe_declare(queue, connection)
        return queue

    @pytest.fixture
    def service_base(self, exchange):

        class Service(object):
            name = "service"

            publish = Publisher(exchange=exchange)

            @dummy
            def proxy(self, *args, **kwargs):
                self.publish(*args, **kwargs)

        return Service

    @pytest.mark.parametrize("option,value,expected", [
        ('delivery_mode', 1, 1),
        ('mandatory', True, True),
        ('priority', 10, 10),
        ('expiration', 10, str(10 * 1000)),
    ])
    def test_delivery_options(
        self, container_factory, rabbit_config, get_message_from_queue, queue,
        service_base, routing_key, option, value, expected
    ):
        container = container_factory(service_base, rabbit_config)
        container.start()

        with entrypoint_hook(container, "proxy") as publish:
            publish("payload", routing_key=routing_key, **{option: value})

        message = get_message_from_queue(queue.name)
        assert message.properties[option] == expected

    def test_mandatory_delivery(
        self, container_factory, rabbit_config, get_message_from_queue, queue,
        service_base
    ):
        container = container_factory(service_base, rabbit_config)
        container.start()

        with entrypoint_hook(container, "proxy") as publish:
            publish("payload", routing_key="missing", mandatory=False)

        with pytest.raises(UndeliverableMessage):
            with entrypoint_hook(container, "proxy") as publish:
                publish("payload", routing_key="missing", mandatory=True)

    @pytest.mark.parametrize("serializer", ['json', 'pickle'])
    def test_serializer(
        self, container_factory, rabbit_config, get_message_from_queue, queue,
        service_base, routing_key, serializer
    ):
        container = container_factory(service_base, rabbit_config)
        container.start()

        payload = {"key": "value"}
        with entrypoint_hook(container, "proxy") as publish:
            publish(payload, routing_key=routing_key, serializer=serializer)

        content_type, content_encoding, expected_body = (
            registry.dumps(payload, serializer=serializer)
        )

        message = get_message_from_queue(queue.name, accept=content_type)
        assert message.body == expected_body

    def test_compression(
        self, container_factory, rabbit_config, get_message_from_queue, queue,
        service_base, routing_key
    ):
        container = container_factory(service_base, rabbit_config)
        container.start()

        payload = {"key": "value"}
        with entrypoint_hook(container, "proxy") as publish:
            publish(payload, routing_key=routing_key, compression="gzip")

        _, expected_content_type = get_encoder('gzip')

        message = get_message_from_queue(queue.name)
        assert message.headers['compression'] == expected_content_type

    def test_content_type(
        self, container_factory, rabbit_config, get_message_from_queue, queue,
        service_base, routing_key
    ):
        container = container_factory(service_base, rabbit_config)
        container.start()

        payload = (
            b'GIF89a\x01\x00\x01\x00\x00\xff\x00,'
            '\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x00;'
        )
        with entrypoint_hook(container, "proxy") as publish:
            publish(payload, routing_key=routing_key, content_type="image/gif")

        message = get_message_from_queue(queue.name)
        assert message.payload == payload
        assert message.properties['content_type'] == 'image/gif'
        assert message.properties['content_encoding'] == 'binary'

    def test_content_encoding(
        self, container_factory, rabbit_config, get_message_from_queue, queue,
        service_base, routing_key
    ):
        container = container_factory(service_base, rabbit_config)
        container.start()

        payload = "A"
        with entrypoint_hook(container, "proxy") as publish:
            publish(
                payload.encode('utf-16'), routing_key=routing_key,
                content_type="application/text", content_encoding="utf-16")

        message = get_message_from_queue(queue.name)
        assert message.payload == payload
        assert message.properties['content_type'] == 'application/text'
        assert message.properties['content_encoding'] == 'utf-16'

    @pytest.mark.parametrize("option,value,expected", [
        ('reply_to', "queue_name", "queue_name"),
        ('message_id', "msg.1", "msg.1"),
        ('type', "type", "type"),
        ('app_id', "app.1", "app.1"),
        ('cluster_id', "cluster.1", "cluster.1"),
        ('correlation_id', "msg.1", "msg.1"),
    ])
    def test_message_properties(
        self, container_factory, rabbit_config, get_message_from_queue, queue,
        service_base, routing_key, option, value, expected
    ):
        container = container_factory(service_base, rabbit_config)
        container.start()

        with entrypoint_hook(container, "proxy") as publish:
            publish("payload", routing_key=routing_key, **{option: value})

        message = get_message_from_queue(queue.name)
        assert message.properties[option] == expected

    def test_timestamp(
        self, container_factory, rabbit_config, get_message_from_queue, queue,
        service_base, routing_key
    ):
        container = container_factory(service_base, rabbit_config)
        container.start()

        now = datetime.now().replace(microsecond=0)

        with entrypoint_hook(container, "proxy") as publish:
            publish("payload", routing_key=routing_key, timestamp=now)

        message = get_message_from_queue(queue.name)
        assert message.properties['timestamp'] == now

    def test_user_id(
        self, container_factory, rabbit_config, get_message_from_queue, queue,
        service_base, routing_key
    ):
        container = container_factory(service_base, rabbit_config)
        container.start()

        user_id = rabbit_config['username']

        # successful case
        with entrypoint_hook(container, "proxy") as publish:
            publish("payload", routing_key=routing_key, user_id=user_id)

        message = get_message_from_queue(queue.name)
        assert message.properties['user_id'] == user_id

        # when user_id does not match the current user, expect an error
        with pytest.raises(PreconditionFailed):
            with entrypoint_hook(container, "proxy") as publish:
                publish("payload", routing_key=routing_key, user_id="invalid")
