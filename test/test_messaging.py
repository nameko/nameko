from __future__ import absolute_import

from contextlib import contextmanager

import eventlet
import pytest
from eventlet.semaphore import Semaphore
from kombu import Exchange, Queue
from kombu.connection import Connection
from kombu.exceptions import OperationalError
from mock import Mock, call, patch
from six.moves import queue

from nameko.amqp.publish import Publisher as PublisherCore
from nameko.amqp.publish import get_producer
from nameko.constants import AMQP_URI_CONFIG_KEY, HEARTBEAT_CONFIG_KEY
from nameko.containers import WorkerContext
from nameko.exceptions import ContainerBeingKilled
from nameko.messaging import (
    Consumer, HeaderDecoder, HeaderEncoder, Publisher, QueueConsumer, consume
)
from nameko.testing.services import dummy, entrypoint_hook, entrypoint_waiter
from nameko.testing.utils import (
    ANY_PARTIAL, DummyProvider, get_extension, unpack_mock_call, wait_for_call
)
from nameko.testing.waiting import wait_for_call as patch_wait

from test import skip_if_no_toxiproxy


foobar_ex = Exchange('foobar_ex', durable=False)
foobar_queue = Queue('foobar_queue', exchange=foobar_ex, durable=False)

CONSUME_TIMEOUT = 1.2  # a bit more than 1 second


@pytest.yield_fixture
def patch_maybe_declare():
    with patch('nameko.messaging.maybe_declare', autospec=True) as patched:
        yield patched


def test_consume_provider(mock_container):

    container = mock_container
    container.shared_extensions = {}
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
    patch_maybe_declare, mock_channel, mock_producer, mock_container
):
    container = mock_container
    container.config = {'AMQP_URI': 'memory://'}
    container.service_name = "srcservice"

    service = Mock()
    worker_ctx = WorkerContext(container, service, DummyProvider("publish"))

    publisher = Publisher(exchange=foobar_ex).bind(container, "publish")

    # test declarations
    publisher.setup()
    assert patch_maybe_declare.call_args_list == [
        call(foobar_ex, mock_channel)
    ]

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
        'declare': publisher.declare,
        'retry': publisher.publisher_cls.retry,
        'retry_policy': publisher.publisher_cls.retry_policy,
        'compression': publisher.publisher_cls.compression,
        'mandatory': publisher.publisher_cls.mandatory,
        'expiration': publisher.publisher_cls.expiration,
        'delivery_mode': publisher.publisher_cls.delivery_mode,
        'priority': publisher.publisher_cls.priority,
        'serializer': publisher.serializer
    }

    assert mock_producer.publish.call_args_list == [
        call(*expected_args, **expected_kwargs)
    ]


@pytest.mark.usefixtures("predictable_call_ids")
@pytest.mark.filterwarnings("ignore:The signature of `Publisher`:DeprecationWarning")
def test_publish_to_queue(
    patch_maybe_declare, mock_producer, mock_channel, mock_container
):
    container = mock_container
    container.config = {'AMQP_URI': 'memory://'}
    container.shared_extensions = {}
    container.service_name = "srcservice"

    ctx_data = {'language': 'en'}
    service = Mock()
    worker_ctx = WorkerContext(
        container, service, DummyProvider("publish"), data=ctx_data)

    publisher = Publisher(queue=foobar_queue).bind(container, "publish")

    # test declarations
    publisher.setup()
    assert patch_maybe_declare.call_args_list == [
        call(foobar_queue, mock_channel)
    ]

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
        'declare': publisher.declare,
        'retry': publisher.publisher_cls.retry,
        'retry_policy': publisher.publisher_cls.retry_policy,
        'compression': publisher.publisher_cls.compression,
        'mandatory': publisher.publisher_cls.mandatory,
        'expiration': publisher.publisher_cls.expiration,
        'delivery_mode': publisher.publisher_cls.delivery_mode,
        'priority': publisher.publisher_cls.priority,
        'serializer': publisher.serializer
    }

    assert mock_producer.publish.call_args_list == [
        call(*expected_args, **expected_kwargs)
    ]


@pytest.mark.usefixtures("predictable_call_ids")
def test_publish_custom_headers(
    mock_container, mock_producer, rabbit_config
):
    container = mock_container
    container.config = rabbit_config
    container.service_name = "srcservice"

    ctx_data = {'language': 'en', 'customheader': 'customvalue'}
    service = Mock()
    worker_ctx = WorkerContext(
        container, service, DummyProvider('method'), data=ctx_data
    )

    publisher = Publisher(exchange=foobar_ex).bind(container, "publish")
    publisher.setup()

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
        'declare': publisher.declare,
        'retry': publisher.publisher_cls.retry,
        'retry_policy': publisher.publisher_cls.retry_policy,
        'compression': publisher.publisher_cls.compression,
        'mandatory': publisher.publisher_cls.mandatory,
        'expiration': publisher.publisher_cls.expiration,
        'delivery_mode': publisher.publisher_cls.delivery_mode,
        'priority': publisher.publisher_cls.priority,
        'serializer': publisher.serializer
    }

    assert mock_producer.publish.call_args_list == [
        call(*expected_args, **expected_kwargs)
    ]


@pytest.mark.filterwarnings("ignore:Attempted to publish unserialisable`:UserWarning")
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

        res = decoder.unpack_message_headers(message)
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
        exchange=foobar_ex, declare=[foobar_queue]
    ).bind(container, "publish")

    publisher.setup()
    publisher.start()

    # test queue, exchange and binding created in rabbit
    exchanges = rabbit_manager.get_exchanges(vhost)
    queues = rabbit_manager.get_queues(vhost)
    bindings = rabbit_manager.get_queue_bindings(vhost, foobar_queue.name)

    assert "foobar_ex" in [exchange['name'] for exchange in exchanges]
    assert "foobar_queue" in [queue['name'] for queue in queues]
    assert "foobar_ex" in [binding['source'] for binding in bindings]

    service.publish = publisher.get_dependency(worker_ctx)
    service.publish("msg")

    # test message published to queue
    messages = rabbit_manager.get_messages(vhost, foobar_queue.name)
    assert ['"msg"'] == [msg['payload'] for msg in messages]

    # test message headers
    assert messages[0]['properties']['headers'] == {
        'nameko.language': 'en',
        'nameko.customheader': 'customvalue',
        'nameko.call_id_stack': ['service.method.0'],
    }


@pytest.mark.usefixtures("predictable_call_ids")
@pytest.mark.filterwarnings("ignore:Attempted to publish unserialisable`:UserWarning")
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
        exchange=foobar_ex, declare=[foobar_queue]).bind(container, "publish")

    publisher.setup()
    publisher.start()

    with pytest.warns(UserWarning):
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

    def spawn_managed_thread(method, identifier=None):
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

    @pytest.fixture
    def lock(self):
        return Semaphore()

    @pytest.fixture
    def tracker(self):
        return Mock()

    @pytest.fixture(autouse=True)
    def container(
        self, container_factory, rabbit_config, toxic_queue_consumer, queue,
        lock, tracker
    ):

        class Service(object):
            name = "service"

            @consume(queue)
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
            toxiproxy.enable()
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

    def test_message_ack_regression(
        self, container, publish, toxiproxy, lock, tracker
    ):
        """ Regression for https://github.com/nameko/nameko/issues/511
        """
        # prevent workers from completing
        lock.acquire()

        # fire entrypoint and block the worker;
        # break connection while the worker is active, then release worker
        with entrypoint_waiter(container, 'echo') as result:
            publish('msg1')
            while not lock._waiters:
                eventlet.sleep()  # pragma: no cover
            toxiproxy.disable()
            # allow connection to close before releasing worker
            eventlet.sleep(.1)
            lock.release()

        # entrypoint will return and attempt to ack initiating message
        assert result.get() == "msg1"

        # enabling connection will re-deliver the initiating message
        # and it will be processed again
        with entrypoint_waiter(container, 'echo') as result:
            toxiproxy.enable()
        assert result.get() == "msg1"

        # connection re-established, container should work again
        with entrypoint_waiter(container, 'echo', timeout=1) as result:
            publish('msg2')
        assert result.get() == 'msg2'

    def test_message_requeue_regression(
        self, container, publish, toxiproxy, lock, tracker
    ):
        """ Regression for https://github.com/nameko/nameko/issues/511
        """
        # turn on requeue_on_error
        consumer = get_extension(container, Consumer)
        consumer.requeue_on_error = True

        # make entrypoint raise the first time it's called so that
        # we attempt to requeue it
        class Boom(Exception):
            pass

        def error_once():
            yield Boom("error")
            while True:
                yield
        tracker.side_effect = error_once()

        # prevent workers from completing
        lock.acquire()

        # fire entrypoint and block the worker;
        # break connection while the worker is active, then release worker
        with entrypoint_waiter(container, 'echo') as result:
            publish('msg1')
            while not lock._waiters:
                eventlet.sleep()  # pragma: no cover
            toxiproxy.disable()
            # allow connection to close before releasing worker
            eventlet.sleep(.1)
            lock.release()

        # entrypoint will return and attempt to requeue initiating message
        with pytest.raises(Boom):
            result.get()

        # enabling connection will re-deliver the initiating message
        # and it will be processed again
        with entrypoint_waiter(container, 'echo', timeout=1) as result:
            toxiproxy.enable()
        assert result.get() == 'msg1'

        # connection re-established, container should work again
        with entrypoint_waiter(container, 'echo', timeout=1) as result:
            publish('msg2')
        assert result.get() == 'msg2'


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
        with patch.object(
            Publisher.publisher_cls, 'use_confirms', new=request.param
        ):
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
        with toxiproxy.disabled():

            payload1 = "payload1"
            with pytest.raises(OperationalError) as exc_info:
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
        with toxiproxy.timeout(500):

            payload1 = "payload1"
            with pytest.raises(OperationalError):  # socket closed
                with entrypoint_hook(publisher_container, 'send') as send:
                    send(payload1)

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

        with toxiproxy.disabled():

            # call 2 fails
            payload2 = "payload2"
            with pytest.raises(IOError):
                with entrypoint_hook(publisher_container, 'send') as send:
                    send(payload2)

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

        with toxiproxy.disabled():

            # call 2 fails
            payload2 = "payload2"
            with pytest.raises(IOError):
                with entrypoint_hook(publisher_container, 'send') as send:
                    send(payload2)

            assert tracker.call_args_list == [
                call("send", payload1),
                call("recv", payload1),
                call("send", payload2),
            ]

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
        with patch_wait(
            Connection,
            "_establish_connection",
            callback=enable_after_retry
        ):
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


class TestBackwardsCompatClassAttrs(object):

    @pytest.mark.parametrize("parameter,value", [
        ('retry', False),
        ('retry_policy', {'max_retries': 999}),
        ('use_confirms', False),
    ])
    def test_attrs_are_applied_as_defaults(
        self, parameter, value, mock_container
    ):
        """ Verify that you can specify some fields by subclassing the
        EventDispatcher DependencyProvider.
        """
        publisher_cls = type(
            "LegacPublisher", (Publisher,), {parameter: value}
        )
        with patch('nameko.messaging.warnings') as warnings:
            mock_container.config = {'AMQP_URI': 'memory://'}
            mock_container.service_name = "service"
            publisher = publisher_cls().bind(mock_container, "publish")
        assert warnings.warn.called
        call_args = warnings.warn.call_args
        assert parameter in unpack_mock_call(call_args).positional[0]

        publisher.setup()
        assert getattr(publisher.publisher, parameter) == value


class TestConfigurability(object):
    """
    Test and demonstrate configuration options for the Publisher
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
        # routing
        'exchange', 'routing_key',
        # delivery options
        'delivery_mode', 'mandatory', 'priority', 'expiration',
        # message options
        'serializer', 'compression',
        # retry policy
        'retry', 'retry_policy',
        # other arbitrary publish kwargs
        'correlation_id', 'user_id', 'bogus_param'
    ])
    def test_regular_parameters(
        self, parameter, mock_container, producer
    ):
        """ Verify that most parameters can be specified at instantiation time,
        and overriden at publish time.
        """
        mock_container.config = {'AMQP_URI': 'memory://localhost'}
        mock_container.service_name = "service"

        worker_ctx = Mock()
        worker_ctx.context_data = {}

        instantiation_value = Mock()
        publish_value = Mock()

        publisher = Publisher(
            **{parameter: instantiation_value}
        ).bind(mock_container, "publish")
        publisher.setup()

        publish = publisher.get_dependency(worker_ctx)

        publish("payload")
        assert producer.publish.call_args[1][parameter] == instantiation_value

        publish("payload", **{parameter: publish_value})
        assert producer.publish.call_args[1][parameter] == publish_value

    @pytest.mark.usefixtures('predictable_call_ids')
    def test_headers(self, mock_container, producer):
        """ Headers provided at publish time are merged with any provided
        at instantiation time. Nameko headers are always present.
        """
        mock_container.config = {
            'AMQP_URI': 'memory://localhost'
        }
        mock_container.service_name = "service"

        # use a real worker context so nameko headers are generated
        service = Mock()
        entrypoint = Mock(method_name="method")
        worker_ctx = WorkerContext(
            mock_container, service, entrypoint, data={'context': 'data'}
        )

        nameko_headers = {
            'nameko.context': 'data',
            'nameko.call_id_stack': ['service.method.0'],
        }

        instantiation_value = {'foo': Mock()}
        publish_value = {'bar': Mock()}

        publisher = Publisher(
            **{'headers': instantiation_value}
        ).bind(mock_container, "publish")
        publisher.setup()

        publish = publisher.get_dependency(worker_ctx)

        def merge_dicts(base, *updates):
            merged = base.copy()
            [merged.update(update) for update in updates]
            return merged

        publish("payload")
        assert producer.publish.call_args[1]['headers'] == merge_dicts(
            nameko_headers, instantiation_value
        )

        publish("payload", headers=publish_value)
        assert producer.publish.call_args[1]['headers'] == merge_dicts(
            nameko_headers, instantiation_value, publish_value
        )

    def test_declare(self, mock_container, producer):
        """ Declarations provided at publish time are merged with any provided
        at instantiation time. Any provided exchange and queue are always
        declared.
        """
        mock_container.config = {
            'AMQP_URI': 'memory://localhost'
        }
        mock_container.service_name = "service"

        worker_ctx = Mock()
        worker_ctx.context_data = {}

        exchange = Mock()

        instantiation_value = [Mock()]
        publish_value = [Mock()]

        publisher = Publisher(
            exchange=exchange, **{'declare': instantiation_value}
        ).bind(mock_container, "publish")
        publisher.setup()

        publish = publisher.get_dependency(worker_ctx)

        publish("payload")
        assert producer.publish.call_args[1]['declare'] == (
            instantiation_value + [exchange]
        )

        publish("payload", declare=publish_value)
        assert producer.publish.call_args[1]['declare'] == (
            instantiation_value + [exchange] + publish_value
        )

    def test_use_confirms(self, mock_container, get_producer):
        """ Verify that publish-confirms can be set as a default specified at
        instantiation time, which can be overriden by a value specified at
        publish time.
        """
        mock_container.config = {'AMQP_URI': 'memory://localhost'}
        mock_container.service_name = "service"

        worker_ctx = Mock()
        worker_ctx.context_data = {}

        publisher = Publisher(
            use_confirms=False
        ).bind(mock_container, "publish")
        publisher.setup()

        publish = publisher.get_dependency(worker_ctx)

        publish("payload")
        use_confirms = get_producer.call_args[0][3].get('confirm_publish')
        assert use_confirms is False

        publish("payload", use_confirms=True)
        use_confirms = get_producer.call_args[0][3].get('confirm_publish')
        assert use_confirms is True


class TestSSL(object):

    @pytest.fixture
    def queue(self,):
        queue = Queue(name="queue")
        return queue

    @pytest.fixture(params=[True, False])
    def rabbit_ssl_config(self, request, rabbit_ssl_config):
        verify_certs = request.param
        if verify_certs is False:
            # remove certificate paths from config
            rabbit_ssl_config['AMQP_SSL'] = True
        return rabbit_ssl_config

    def test_consume_over_ssl(
        self, container_factory, rabbit_ssl_config, rabbit_config, queue
    ):
        class Service(object):
            name = "service"

            @consume(queue)
            def echo(self, payload):
                return payload

        container = container_factory(Service, rabbit_ssl_config)
        container.start()

        publisher = PublisherCore(rabbit_config['AMQP_URI'])

        with entrypoint_waiter(container, 'echo') as result:
            publisher.publish("payload", routing_key=queue.name)
        assert result.get() == "payload"

    def test_publisher_over_ssl(
        self, container_factory, rabbit_ssl_config, rabbit_config, queue
    ):
        class PublisherService(object):
            name = "publisher"

            publish = Publisher()

            @dummy
            def method(self, payload):
                return self.publish(payload, routing_key=queue.name)

        class ConsumerService(object):
            name = "consumer"

            @consume(queue)
            def echo(self, payload):
                return payload

        publisher = container_factory(PublisherService, rabbit_ssl_config)
        publisher.start()

        consumer = container_factory(ConsumerService, rabbit_config)
        consumer.start()

        with entrypoint_waiter(consumer, 'echo') as result:
            with entrypoint_hook(publisher, 'method') as publish:
                publish("payload")
        assert result.get() == "payload"
