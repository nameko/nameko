from __future__ import absolute_import

from contextlib import contextmanager

import eventlet
import pytest
from eventlet.event import Event
from eventlet.semaphore import Semaphore
from kombu import Exchange, Queue
from kombu.connection import Connection
from kombu.exceptions import OperationalError
from mock import Mock, call, patch
from six.moves import queue

from nameko import config
from nameko.amqp.consume import Consumer as ConsumerCore
from nameko.amqp.publish import Publisher as PublisherCore
from nameko.amqp.publish import get_producer
from nameko.constants import AMQP_URI_CONFIG_KEY, HEARTBEAT_CONFIG_KEY
from nameko.containers import WorkerContext
from nameko.exceptions import ContainerBeingKilled
from nameko.messaging import (
    Consumer, Publisher, consume, decode_from_headers, encode_to_headers
)
from nameko.testing.services import dummy, entrypoint_hook, entrypoint_waiter
from nameko.testing.utils import (
    ANY_PARTIAL, DummyProvider, get_extension, wait_for_call
)
from nameko.testing.waiting import wait_for_call as patch_wait
from nameko.utils.retry import retry

from test import skip_if_no_toxiproxy


foobar_ex = Exchange('foobar_ex', durable=False)
foobar_queue = Queue('foobar_queue', exchange=foobar_ex, durable=False)

CONSUME_TIMEOUT = 1.2  # a bit more than 1 second


@pytest.yield_fixture
def patch_maybe_declare():
    with patch('nameko.messaging.maybe_declare', autospec=True) as patched:
        yield patched


@pytest.mark.usefixtures("rabbit_config")
@pytest.mark.usefixtures("predictable_call_ids")
def test_publish_to_exchange(
    patch_maybe_declare, mock_channel, mock_producer, mock_container
):
    container = mock_container
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
        'serializer': publisher.publisher_cls.serializer
    }

    assert mock_producer.publish.call_args_list == [
        call(*expected_args, **expected_kwargs)
    ]


@pytest.mark.usefixtures("rabbit_config")
@pytest.mark.usefixtures("predictable_call_ids")
def test_publish_custom_headers(mock_container, mock_producer):
    container = mock_container
    container.service_name = "srcservice"

    ctx_data = {'language': 'en', 'customheader': 'customvalue'}
    service = Mock()
    worker_ctx = WorkerContext(
        container, service, DummyProvider('method'), data=ctx_data
    )

    publisher = Publisher(
        exchange=foobar_ex, declare=[foobar_queue]).bind(container, "publish")
    publisher.setup()

    msg = "msg"
    service.publish = publisher.get_dependency(worker_ctx)

    # context data changes are reflected up to the point of publishing
    worker_ctx.data['language'] = 'fr'

    service.publish(msg, publish_kwarg="value")

    expected_args = ('msg',)
    expected_headers = {
        'nameko.language': 'fr',
        'nameko.customheader': 'customvalue',
        'nameko.call_id_stack': ['srcservice.method.0'],
    }
    expected_kwargs = {
        'publish_kwarg': "value",
        'exchange': foobar_ex,
        'headers': expected_headers,
        'declare': publisher.declare,
        'retry': publisher.publisher_cls.retry,
        'retry_policy': publisher.publisher_cls.retry_policy,
        'compression': publisher.publisher_cls.compression,
        'mandatory': publisher.publisher_cls.mandatory,
        'expiration': publisher.publisher_cls.expiration,
        'delivery_mode': publisher.publisher_cls.delivery_mode,
        'priority': publisher.publisher_cls.priority,
        'serializer': publisher.publisher_cls.serializer
    }

    assert mock_producer.publish.call_args_list == [
        call(*expected_args, **expected_kwargs)
    ]


@pytest.mark.usefixtures('empty_config')
def test_encode_headers():

    context_data = {
        'foo': 'FOO',
        'bar': 'BAR',
        'baz': 'BAZ',
        # unserialisable, shouldn't be included in the processed headers
        'none': None,
    }

    res = encode_to_headers(context_data, prefix="testprefix")
    assert res == {
        'testprefix.foo': 'FOO',
        'testprefix.bar': 'BAR',
        'testprefix.baz': 'BAZ',
    }


def test_decode_headers():

    headers = {
        'testprefix.foo': 'FOO',
        'testprefix.bar': 'BAR',
        'testprefix.baz': 'BAZ',
        'differentprefix.foo': 'XXX',
        'testprefix.call_id_stack': ['a', 'b', 'c'],
    }

    res = decode_from_headers(headers, prefix="testprefix")
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

@pytest.mark.usefixtures("rabbit_config")
@pytest.mark.usefixtures("predictable_call_ids")
def test_publish_to_rabbit(rabbit_manager, get_vhost, mock_container):

    vhost = get_vhost(config['AMQP_URI'])

    container = mock_container
    container.service_name = "service"

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


@pytest.mark.usefixtures("rabbit_config")
@pytest.mark.usefixtures("predictable_call_ids")
def test_unserialisable_headers(rabbit_manager, get_vhost, mock_container):

    vhost = get_vhost(config['AMQP_URI'])

    container = mock_container
    container.service_name = "service"
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

    service.publish = publisher.get_dependency(worker_ctx)
    service.publish("msg")
    messages = rabbit_manager.get_messages(vhost, foobar_queue.name)

    assert messages[0]['properties']['headers'] == {
        'nameko.language': 'en',
        'nameko.call_id_stack': ['service.method.0'],
        # no `customheader`
    }


@pytest.mark.usefixtures("rabbit_config")
def test_consume_from_rabbit(get_vhost, rabbit_manager, mock_container):

    vhost = get_vhost(config['AMQP_URI'])

    container = mock_container
    container.shared_extensions = {}
    container.worker_ctx_cls = WorkerContext
    container.service_name = "service"
    container.max_workers = 10

    content_type = 'application/data'
    container.accept = [content_type]

    def spawn_managed_thread(method, identifier=None):
        return eventlet.spawn(method)

    container.spawn_managed_thread = spawn_managed_thread

    worker_ctx = WorkerContext(container, None, DummyProvider())

    consumer = Consumer(
        queue=foobar_queue, requeue_on_error=False).bind(container, "publish")

    consumer.setup()
    consumer.start()

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

    consumer.stop()


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
            ConsumerCore, 'establish_connection', new=establish_connection
        ):
            yield

    @pytest.yield_fixture
    def toxic_consumer(self, toxiproxy):
        with patch.object(Consumer, 'amqp_uri', new=toxiproxy.uri):
            yield

    @pytest.fixture
    def queue(self,):
        queue = Queue(name="queue")
        return queue

    @pytest.fixture
    def publish(self, rabbit_config, queue):
        amqp_uri = config[AMQP_URI_CONFIG_KEY]

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
        self, container_factory, rabbit_config, toxic_consumer, queue,
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

        # very fast heartbeat (2 seconds)
        with config.patch({HEARTBEAT_CONFIG_KEY: 2}):
            container = container_factory(Service)
            container.start()

            # we have to let the container connect before disconnecting
            # otherwise we end up in retry_over_time trying to make the
            # initial connection; we get stuck there because it has a
            # function-local copy of "on_connection_error" that is never patched
            eventlet.sleep(.05)

            yield container

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
        consumer = get_extension(container, Consumer)

        def reset(args, kwargs, result, exc_info):
            toxiproxy.enable()
            return True

        with patch_wait(
            consumer.consumer, 'on_connection_error', callback=reset
        ):
            toxiproxy.disable()

        # connection re-established
        msg = "foo"
        with entrypoint_waiter(container, 'echo') as result:
            publish(msg)
        assert result.get() == msg

    def test_upstream_timeout(self, container, publish, toxiproxy):
        """ Verify we detect and recover from sockets timing out.

        This failure mode means that the socket between the consumer and the
        rabbit broker times out after `timeout` milliseconds and then closes.

        Attempting to read from the socket after it's closed raises a
        socket.error and the connection will be re-established. If `timeout`
        is longer than twice the heartbeat interval, the behaviour is the same
        as in `test_upstream_blackhole` below.
        """
        consumer = get_extension(container, Consumer)

        def reset(args, kwargs, result, exc_info):
            toxiproxy.reset_timeout()
            return True

        with patch_wait(
            consumer.consumer, 'on_connection_error', callback=reset
        ):
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
        consumer = get_extension(container, Consumer)

        def reset(args, kwargs, result, exc_info):
            toxiproxy.reset_timeout()
            return True

        with patch_wait(
            consumer.consumer, 'on_connection_error', callback=reset
        ):
            toxiproxy.set_timeout(timeout=0)

        # connection re-established
        msg = "foo"
        with entrypoint_waiter(container, 'echo') as result:
            publish(msg)
        assert result.get() == msg

    def test_downstream_timeout(self, container, publish, toxiproxy):
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
        consumer = get_extension(container, Consumer)

        def reset(args, kwargs, result, exc_info):
            toxiproxy.reset_timeout()
            return True

        with patch_wait(
            consumer.consumer, 'on_connection_error', callback=reset
        ):
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

        consumer = get_extension(container, Consumer)

        def reset(args, kwargs, result, exc_info):
            toxiproxy.reset_timeout()
            return True

        with patch_wait(
            consumer.consumer, 'on_connection_error', callback=reset
        ):
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

    @pytest.yield_fixture(params=[True, False])
    def use_confirms(self, request):
        with patch.object(
            Publisher.publisher_cls, 'use_confirms', new=request.param
        ):
            yield request.param

    @pytest.yield_fixture
    def publisher_container(
        self, request, container_factory, tracker, rabbit_config, toxiproxy
    ):
        retry = False
        if "publish_retry" in request.keywords:
            retry = True

        class Service(object):
            name = "publish"

            publish = Publisher(uri=toxiproxy.uri)

            @dummy
            def send(self, payload):
                tracker("send", payload)
                self.publish(payload, routing_key="test_queue", retry=retry)

        container = container_factory(Service)
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

        container = container_factory(Service)
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


class TestConsumerConfigurability(object):
    """
    Test and demonstrate configuration options for the Consumer
    """

    @pytest.mark.usefixtures("memory_rabbit_config")
    def test_heartbeat(self, mock_container):
        mock_container.service_name = "service"

        value = 999
        queue = Mock()

        consumer = Consumer(
            queue, heartbeat=value
        ).bind(mock_container, "method")
        consumer.setup()

        assert consumer.consumer.connection.heartbeat == value

    @pytest.mark.usefixtures("memory_rabbit_config")
    def test_prefetch_count(self, mock_container):
        mock_container.service_name = "service"

        value = 999
        queue = Mock()

        consumer = Consumer(
            queue, prefetch_count=value
        ).bind(mock_container, "method")
        consumer.setup()

        assert consumer.consumer.prefetch_count == value

    @pytest.mark.usefixtures("memory_rabbit_config")
    def test_accept(self, mock_container):
        mock_container.service_name = "service"

        value = ['yaml', 'json']
        queue = Mock()

        consumer = Consumer(
            queue, accept=value
        ).bind(mock_container, "method")
        consumer.setup()

        assert consumer.consumer.accept == value


class TestPublisherConfigurability(object):
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

    @pytest.mark.usefixtures("memory_rabbit_config")
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
        and overridden at publish time.
        """
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

    @pytest.mark.usefixtures("memory_rabbit_config")
    @pytest.mark.usefixtures('predictable_call_ids')
    def test_headers(self, mock_container, producer):
        """ Headers provided at publish time are merged with any provided
        at instantiation time. Nameko headers are always present.
        """
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

    @pytest.mark.usefixtures("memory_rabbit_config")
    def test_declare(self, mock_container, producer):
        """ Declarations provided at publish time are merged with any provided
        at instantiation time. Any provided exchange and queue are always
        declared.
        """
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

    @pytest.mark.usefixtures("memory_rabbit_config")
    def test_use_confirms(self, mock_container, get_producer):
        """ Verify that publish-confirms can be set as a default specified at
        instantiation time, which can be overridden by a value specified at
        publish time.
        """
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


class TestPrefetchCount(object):

    @pytest.mark.usefixtures("rabbit_config")
    @config.patch({'PREFETCH_COUNT': 1})
    def test_prefetch_count(
        self, rabbit_manager, get_vhost, container_factory
    ):

        vhost = get_vhost(config['AMQP_URI'])

        messages = []

        exchange = Exchange('exchange')
        queue = Queue('queue', exchange=exchange, auto_delete=False)

        class LimitedConsumer1(Consumer):

            def handle_message(self, body, message):
                consumer_continue.wait()
                super(LimitedConsumer1, self).handle_message(body, message)

        class LimitedConsumer2(Consumer):

            def handle_message(self, body, message):
                messages.append(body)
                super(LimitedConsumer2, self).handle_message(body, message)

        class Service(object):
            name = "service"

            @LimitedConsumer1.decorator(queue=queue)
            @LimitedConsumer2.decorator(queue=queue)
            def handle(self, payload):
                pass

        container = container_factory(Service)
        container.start()

        consumer_continue = Event()

        # the two handlers would ordinarily take alternating messages, but are
        # limited to holding one un-ACKed message. Since Handler1 never ACKs,
        # it only ever gets one message, and Handler2 gets the others.

        def wait_for_expected(worker_ctx, res, exc_info):
            return {'m3', 'm4', 'm5'}.issubset(set(messages))

        with entrypoint_waiter(
            container, 'handle', callback=wait_for_expected
        ):
            properties = {'content_type': 'application/data'}
            for message in ('m1', 'm2', 'm3', 'm4', 'm5'):
                rabbit_manager.publish(
                    vhost, 'exchange', '', message, properties=properties
                )

        # we don't know which handler picked up the first message,
        # but all the others should've been handled by Handler2
        assert messages[-3:] == ['m3', 'm4', 'm5']

        # release the waiting consumer
        consumer_continue.send(None)


class TestContainerBeingKilled(object):

    @pytest.fixture
    def publisher(self, amqp_uri):
        return PublisherCore(amqp_uri)

    @pytest.mark.usefixtures("rabbit_config")
    def test_container_killed(self, container_factory, publisher):
        queue = Queue('queue')

        class Service(object):
            name = "service"

            @consume(queue)
            def method(self, payload):
                pass  # pragma: no cover

        container = container_factory(Service)
        container.start()

        # check message is requeued if container throws ContainerBeingKilled
        with patch.object(container, 'spawn_worker') as spawn_worker:
            spawn_worker.side_effect = ContainerBeingKilled()

            with patch_wait(ConsumerCore, 'requeue_message'):
                publisher.publish("payload", routing_key=queue.name)


class TestSSL(object):

    @pytest.fixture
    def queue(self,):
        queue = Queue(name="queue")
        return queue

    @pytest.fixture(params=[True, False])
    def rabbit_ssl_options(self, request, rabbit_ssl_options):
        verify_certs = request.param
        if verify_certs is False:
            # remove certificate paths from config
            options = True
        else:
            options = rabbit_ssl_options
        return options

    @pytest.mark.usefixtures("rabbit_ssl_config")
    def test_consume_over_ssl(self, container_factory, queue, rabbit_uri):

        class Service(object):
            name = "service"

            @consume(queue)
            def echo(self, payload):
                return payload

        container = container_factory(Service)
        container.start()

        publisher = PublisherCore(rabbit_uri)

        with entrypoint_waiter(container, 'echo') as result:
            publisher.publish("payload", routing_key=queue.name)
        assert result.get() == "payload"

    @pytest.mark.usefixtures("rabbit_config")
    def test_publisher_over_ssl(
        self, container_factory, queue, rabbit_ssl_uri, rabbit_ssl_options
    ):
        class PublisherService(object):
            name = "publisher"

            publish = Publisher(uri=rabbit_ssl_uri, ssl=rabbit_ssl_options)

            @dummy
            def method(self, payload):
                return self.publish(payload, routing_key=queue.name)

        class ConsumerService(object):
            name = "consumer"

            @consume(queue)
            def echo(self, payload):
                return payload

        publisher = container_factory(PublisherService)
        publisher.start()

        consumer = container_factory(ConsumerService)
        consumer.start()

        with entrypoint_waiter(consumer, 'echo') as result:
            with entrypoint_hook(publisher, 'method') as publish:
                publish("payload")
        assert result.get() == "payload"


@pytest.mark.usefixtures("rabbit_config")
def test_stop_with_active_worker(container_factory, queue_info):
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

        @consume(Queue(name="queue"))
        def method(self, payload):
            block.wait()

    container = container_factory(Service)
    container.start()

    publisher = PublisherCore(config['AMQP_URI'])
    publisher.publish("payload", routing_key="queue")

    gt = eventlet.spawn(container.stop)

    @retry
    def consumer_removed():
        info = queue_info('queue')
        assert info.consumer_count == 0
        assert info.message_count == 1

    consumer_removed()

    assert not gt.dead
    block.send(True)

    gt.wait()
    assert gt.dead


class TestEntrypointArguments:

    @pytest.mark.usefixtures("rabbit_config")
    def test_expected_exceptions_and_sensitive_arguments(
        self, container_factory
    ):

        class Boom(Exception):
            pass

        class Service(object):
            name = "service"

            @consume(
                Queue(name="queue"),
                expected_exceptions=Boom,
                sensitive_arguments=["payload"]
            )
            def method(self, payload):
                pass  # pragma: no cover

        container = container_factory(Service)
        container.start()

        entrypoint = get_extension(container, Consumer)
        assert entrypoint.expected_exceptions == Boom
        assert entrypoint.sensitive_arguments == ["payload"]
