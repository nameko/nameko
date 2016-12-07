from contextlib import contextmanager
import socket

import eventlet
import pytest
from kombu import Exchange, Queue
from kombu.connection import Connection
from mock import Mock, call, patch

from nameko.amqp import get_producer, UndeliverableMessage
from nameko.constants import (
    AMQP_URI_CONFIG_KEY, DEFAULT_RETRY_POLICY, HEARTBEAT_CONFIG_KEY)
from nameko.containers import WorkerContext
from nameko.exceptions import ContainerBeingKilled
from nameko.messaging import (
    Consumer, HeaderDecoder, HeaderEncoder, Publisher, QueueConsumer, consume)
from nameko.testing.services import dummy, entrypoint_hook, entrypoint_waiter
from nameko.testing.utils import (
    ANY_PARTIAL, DummyProvider, get_extension, wait_for_call)
from nameko.testing.waiting import wait_for_call as patch_wait

from test import skip_if_no_toxiproxy

foobar_ex = Exchange('foobar_ex', durable=False)
foobar_queue = Queue('foobar_queue', exchange=foobar_ex, durable=False)

CONSUME_TIMEOUT = 1


@pytest.yield_fixture
def warnings():
    with patch('nameko.messaging.warnings') as patched:
        yield patched


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
        serializer=container.serializer, mandatory=False,
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
        serializer=container.serializer, mandatory=False,
        retry_policy=DEFAULT_RETRY_POLICY, publish_kwarg="value")


@pytest.mark.usefixtures("predictable_call_ids")
def test_publish_custom_headers(
    mock_container, maybe_declare, mock_producer, mock_connection
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
        serializer=container.serializer, mandatory=False,
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


class TestMandatoryDelivery(object):
    """ Test and demonstrate the mandatory delivery flag.

    Publishing a message should raise an exception when mandatory delivery
    is requested and there is no destination queue, as long as publish-confirms
    are enabled.
    """
    @pytest.fixture()
    def container(self, container_factory, rabbit_config):

        class Service(object):
            name = "publisher"

            publish = Publisher()

            @dummy
            def method(self, *args, **kwargs):
                self.publish(*args, **kwargs)

        container = container_factory(Service, rabbit_config)
        container.start()
        return container

    def test_default(self, container):
        # messages are not mandatory by default;
        # no error when routing to a non-existent queue
        with entrypoint_hook(container, 'method') as publish:
            publish("payload", routing_key="bogus")

    def test_mandatory_delivery(self, container):
        # requesting mandatory delivery will result in an exception
        # if there is no bound queue to receive the message
        with pytest.raises(UndeliverableMessage):
            with entrypoint_hook(container, 'method') as publish:
                publish("payload", routing_key="bogus", mandatory=True)

    def test_confirms_disabled(
        self, container_factory, rabbit_config, warnings
    ):

        class UnconfirmedPublisher(Publisher):
            use_confirms = False

        class Service(object):
            name = "service"

            publish = UnconfirmedPublisher()

            @dummy
            def method(self, *args, **kwargs):
                self.publish(*args, **kwargs)

        container = container_factory(Service, rabbit_config)
        container.start()

        # no exception will be raised if confirms are disabled,
        # even when mandatory delivery is requested,
        # but there will be a warning raised
        with entrypoint_hook(container, 'method') as publish:
            publish("payload", routing_key="bogus", mandatory=True)
        assert warnings.warn.called


@skip_if_no_toxiproxy
class TestConsumerDisconnections(object):
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
            QueueConsumer, 'establish_connection', new=establish_connection
        ):
            yield

    @pytest.yield_fixture
    def toxic_queue_consumer(self, toxiproxy):
        with patch.object(QueueConsumer, 'amqp_uri', new=toxiproxy.uri):
            yield

    @pytest.fixture
    def queue(self,):
        queue = Queue(name="queue")
        return queue

    @pytest.fixture
    def publish(self, rabbit_config, queue):
        amqp_uri = rabbit_config[AMQP_URI_CONFIG_KEY]

        def publish(msg):
            with get_producer(amqp_uri) as producer:
                producer.publish(
                    msg,
                    serializer="json",
                    routing_key=queue.name
                )
        return publish

    @pytest.fixture(autouse=True)
    def container(
        self, container_factory, rabbit_config, toxic_queue_consumer, queue
    ):

        class Service(object):
            name = "service"

            @consume(queue)
            def echo(self, arg):
                return arg

        # very fast heartbeat
        config = rabbit_config
        config[HEARTBEAT_CONFIG_KEY] = 2  # seconds

        container = container_factory(Service, config)
        container.start()

        return container

    def test_normal(self, container, publish):
        msg = "foo"
        with entrypoint_waiter(container, 'echo') as result:
            publish(msg)
        assert result.get() == msg

    def test_down(self, container, publish, toxiproxy):
        """ Verify we detect and recover from closed sockets.

        This failure mode closes the socket between the consumer and the
        rabbit broker.

        Attempting to read from the closed socket raises a socket.error
        and the connection is re-established.
        """
        queue_consumer = get_extension(container, QueueConsumer)

        def reset(args, kwargs, result, exc_info):
            toxiproxy.reset()
            return True

        with patch_wait(queue_consumer, 'on_connection_error', callback=reset):
            toxiproxy.disable()

        # connection re-established
        msg = "foo"
        with entrypoint_waiter(container, 'echo') as result:
            publish(msg)
        assert result.get() == msg

    def test_upstream_timeout(self, container, publish, toxiproxy):
        """ Verify we detect and recover from sockets timing out.

        This failure mode means that the socket between the consumer and the
        rabbit broker times for out `timeout` milliseconds and then closes.

        Attempting to read from the socket after it's closed raises a
        socket.error and the connection will be re-established. If `timeout`
        is longer than twice the heartbeat interval, the behaviour is the same
        as in `test_upstream_blackhole` below.
        """
        queue_consumer = get_extension(container, QueueConsumer)

        def reset(args, kwargs, result, exc_info):
            toxiproxy.reset_timeout()
            return True

        with patch_wait(queue_consumer, 'on_connection_error', callback=reset):
            toxiproxy.set_timeout(timeout=100)

        # connection re-established
        msg = "foo"
        with entrypoint_waiter(container, 'echo') as result:
            publish(msg)
        assert result.get() == msg

    def test_upstream_blackhole(self, container, publish, toxiproxy):
        """ Verify we detect and recover from sockets losing data.

        This failure mode means that all data sent from the consumer to the
        rabbit broker is lost, but the socket remains open.

        Heartbeats sent from the consumer are not received by the broker. After
        two beats are missed the broker closes the connection, and subsequent
        reads from the socket raise a socket.error, so the connection is
        re-established.
        """
        queue_consumer = get_extension(container, QueueConsumer)

        def reset(args, kwargs, result, exc_info):
            toxiproxy.reset_timeout()
            return True

        with patch_wait(queue_consumer, 'on_connection_error', callback=reset):
            toxiproxy.set_timeout(timeout=0)

        # connection re-established
        msg = "foo"
        with entrypoint_waiter(container, 'echo') as result:
            publish(msg)
        assert result.get() == msg

    def test_downstream_timeout(self, container, publish, toxiproxy):
        """ Verify we detect and recover from sockets timing out.

        This failure mode means that the socket between the rabbit broker and
        the consumer times for out `timeout` milliseconds and then closes.

        Attempting to read from the socket after it's closed raises a
        socket.error and the connection will be re-established. If `timeout`
        is longer than twice the heartbeat interval, the behaviour is the same
        as in `test_downstream_blackhole` below, except that the consumer
        cancel will eventually (`timeout` milliseconds) raise a socket.error,
        which is ignored, allowing the teardown to continue.

        See :meth:`kombu.messsaging.Consumer.__exit__`
        """
        queue_consumer = get_extension(container, QueueConsumer)

        def reset(args, kwargs, result, exc_info):
            toxiproxy.reset_timeout()
            return True

        with patch_wait(queue_consumer, 'on_connection_error', callback=reset):
            toxiproxy.set_timeout(stream="downstream", timeout=100)

        # connection re-established
        msg = "foo"
        with entrypoint_waiter(container, 'echo') as result:
            publish(msg)
        assert result.get() == msg

    def test_downstream_blackhole(
        self, container, publish, toxiproxy
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

        queue_consumer = get_extension(container, QueueConsumer)

        def reset(args, kwargs, result, exc_info):
            toxiproxy.reset_timeout()
            return True

        with patch_wait(queue_consumer, 'on_connection_error', callback=reset):
            toxiproxy.set_timeout(stream="downstream", timeout=0)

        # connection re-established
        msg = "foo"
        with entrypoint_waiter(container, 'echo') as result:
            publish(msg)
        assert result.get() == msg


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
