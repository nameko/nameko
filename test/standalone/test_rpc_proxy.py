import socket

import eventlet
import pytest
from eventlet.event import Event
from kombu.connection import Connection
from kombu.exceptions import OperationalError
from kombu.message import Message
from mock import Mock, call

from nameko.containers import WorkerContext
from nameko.exceptions import RemoteError, RpcTimeout
from nameko.extensions import DependencyProvider
from nameko.rpc import MethodProxy, Responder, get_rpc_exchange, rpc
from nameko.standalone.rpc import (
    ClusterRpcProxy, ConsumeEvent, ServiceRpcProxy
)
from nameko.testing.waiting import wait_for_call

from test import skip_if_no_toxiproxy


# uses autospec on method; needs newer mock for py3
try:
    from unittest.mock import patch
except ImportError:  # pragma: no cover
    from mock import patch


class ContextReader(DependencyProvider):
    """ Access values from the worker context data.

    This is a test facilty! Write specific Dependencies to make use of
    values in ``WorkerContext.data``, don't expose it directly.
    """

    def get_dependency(self, worker_ctx):
        def get_context_value(key):
            return worker_ctx.data.get(key)
        return get_context_value


class FooService(object):
    name = 'foobar'

    get_context_value = ContextReader()

    @rpc
    def spam(self, ham):
        return ham

    @rpc
    def broken(self):
        raise ExampleError('broken')

    @rpc
    def get_context_data(self, name):
        return self.get_context_value(name)

    @rpc
    def sleep(self, seconds=0):
        eventlet.sleep(seconds)
        return seconds


class ExampleError(Exception):
    pass


class CustomWorkerContext(WorkerContext):
    context_keys = ("custom_header",)


def test_proxy(container_factory, rabbit_config):

    container = container_factory(FooService, rabbit_config)
    container.start()

    with ServiceRpcProxy('foobar', rabbit_config) as foo:
        assert foo.spam(ham='eggs') == 'eggs'
        assert foo.spam(ham='eggs') == 'eggs'  # test re-use


def test_proxy_manual_start_stop(container_factory, rabbit_config):

    container = container_factory(FooService, rabbit_config)
    container.start()

    foobar_proxy = ServiceRpcProxy('foobar', rabbit_config)
    foo = foobar_proxy.start()
    assert foo.spam(ham='eggs') == 'eggs'
    assert foo.spam(ham='eggs') == 'eggs'  # test re-use
    foobar_proxy.stop()


def test_proxy_context_data(container_factory, rabbit_config):

    container = container_factory(FooService, rabbit_config)
    container.start()

    context_data = {'language': 'en'}
    with ServiceRpcProxy('foobar', rabbit_config, context_data) as foo:
        assert foo.get_context_data('language') == 'en'

    context_data = {'language': 'fr'}
    with ServiceRpcProxy('foobar', rabbit_config, context_data) as foo:
        assert foo.get_context_data('language') == 'fr'


def test_proxy_remote_error(container_factory, rabbit_config):

    container = container_factory(FooService, rabbit_config)
    container.start()

    with ServiceRpcProxy("foobar", rabbit_config) as proxy:
        with pytest.raises(RemoteError) as exc_info:
            proxy.broken()
        assert exc_info.value.exc_type == "ExampleError"


def test_proxy_connection_error(container_factory, rabbit_config):

    container = container_factory(FooService, rabbit_config)
    container.start()

    with ServiceRpcProxy("foobar", rabbit_config) as proxy:
        queue_consumer = proxy.reply_listener.queue_consumer
        with patch.object(queue_consumer, 'get_message', autospec=True) as get:
            get.side_effect = socket.error
            with pytest.raises(socket.error):
                proxy.spam("")


@patch('nameko.rpc.RPC_REPLY_QUEUE_TTL', new=100)
def test_reply_queue_removed_on_expiry(
    rabbit_manager, rabbit_config, container_factory
):
    def list_queues():
        vhost = rabbit_config['vhost']
        return [
            queue['name']
            for queue in rabbit_manager.get_queues(vhost=vhost)
        ]

    container = container_factory(FooService, rabbit_config)
    container.start()

    queues_before = list_queues()

    with ServiceRpcProxy('foobar', rabbit_config) as foo:
        queues_during = list_queues()
        assert foo.spam(ham='eggs') == 'eggs'

    eventlet.sleep(0.2)  # sleep for >TTL
    queues_after = list_queues()

    assert queues_before != queues_during
    assert queues_after == queues_before

    # check proxy re-use
    with ServiceRpcProxy('foobar', rabbit_config) as foo:
        assert foo.spam(ham='eggs') == 'eggs'
        assert foo.spam(ham='eggs') == 'eggs'

    eventlet.sleep(0.2)  # sleep for >TTL
    queues_after = list_queues()
    assert queues_after == queues_before


@patch('nameko.rpc.RPC_REPLY_QUEUE_TTL', new=100)
def test_reply_queue_not_removed_while_in_use(
    rabbit_manager, rabbit_config, container_factory
):
    def list_queues():
        vhost = rabbit_config['vhost']
        return [
            queue['name']
            for queue in rabbit_manager.get_queues(vhost=vhost)
        ]

    container = container_factory(FooService, rabbit_config)
    container.start()

    # check proxy re-use
    with ServiceRpcProxy('foobar', rabbit_config) as foo:
        queues_before = list_queues()
        # sleep for 2x TTL
        assert foo.sleep(0.2) == 0.2
        queues_between = list_queues()
        assert foo.spam(ham='eggs') == 'eggs'
        queues_after = list_queues()

    assert queues_before == queues_between == queues_after


@skip_if_no_toxiproxy
class TestDisconnectedWhileWaitingForReply(object):

    @patch('nameko.rpc.RPC_REPLY_QUEUE_TTL', new=100)
    def test_reply_queue_removed_while_disconnected_with_pending_reply(
        self, rabbit_config, container_factory, toxiproxy
    ):
        """ Not possible to test this scenario with the current design.
        We attempt to _setup_consumer immediately on disconnection, without
        any kind of retry and only for two attempts; the broker will never
        have expired the queue during that window.

        It will be possible once to write test this scenario once the
        `rpc-refactor` branch lands. This test will then become very similar
        to test/test_rpc.py::TestDisconnectedWhileWaitingForReply.
        """
        pytest.skip("Not possible to test with current implementation")


def test_unexpected_correlation_id(container_factory, rabbit_config):
    container = container_factory(FooService, rabbit_config)
    container.start()

    with ServiceRpcProxy("foobar", rabbit_config) as proxy:

        message = Message(channel=None, properties={
            'reply_to': proxy.reply_listener.routing_key,
            'correlation_id': 'invalid',
            'content_type': 'application/json'
        })
        amqp_uri = container.config['AMQP_URI']
        exchange = get_rpc_exchange(container.config)

        responder = Responder(amqp_uri, exchange, "json", message)
        with patch('nameko.standalone.rpc._logger', autospec=True) as logger:
            responder.send_response(None, None)
            assert proxy.spam(ham='eggs') == 'eggs'
            assert logger.debug.call_count == 1


def test_async_rpc(container_factory, rabbit_config):

    container = container_factory(FooService, rabbit_config)
    container.start()

    with ServiceRpcProxy('foobar', rabbit_config) as foo:
        rep1 = foo.spam.call_async(ham=1)
        rep2 = foo.spam.call_async(ham=2)
        rep3 = foo.spam.call_async(ham=3)
        rep4 = foo.spam.call_async(ham=4)
        rep5 = foo.spam.call_async(ham=5)
        assert rep2.result() == 2
        assert rep3.result() == 3
        assert rep1.result() == 1
        assert rep4.result() == 4
        assert rep5.result() == 5


def test_multiple_proxies(container_factory, rabbit_config):
    container = container_factory(FooService, rabbit_config)
    container.start()

    with ServiceRpcProxy('foobar', rabbit_config) as proxy1:
        res1 = proxy1.spam.call_async(ham=1)

        with ServiceRpcProxy('foobar', rabbit_config) as proxy2:
            res2 = proxy2.spam.call_async(ham=2)

            assert res1.result() == 1
            assert res2.result() == 2


def test_multiple_calls_to_result(container_factory, rabbit_config):
    container = container_factory(FooService, rabbit_config)
    container.start()

    with ServiceRpcProxy('foobar', rabbit_config) as proxy:
        res = proxy.spam.call_async(ham=1)
        res.result()
        res.result()


@skip_if_no_toxiproxy
class TestDisconnectWithPendingReply(object):

    @pytest.yield_fixture
    def toxic_rpc_proxy(self, rabbit_config, toxiproxy):
        rabbit_config['AMQP_URI'] = toxiproxy.uri
        with ClusterRpcProxy(rabbit_config) as proxy:
            yield proxy

    def test_disconnect_and_successfully_reconnect(
        self, container_factory, rabbit_manager, rabbit_config,
        toxic_rpc_proxy, toxiproxy
    ):
        block = Event()

        class Service(object):
            name = "service"

            @rpc
            def method(self, arg):
                block.wait()
                return arg

        container = container_factory(Service, rabbit_config)
        container.start()

        # make an async call that will block,
        # wait for the worker to have spawned
        with wait_for_call(container, 'spawn_worker'):
            res = toxic_rpc_proxy.service.method.call_async('msg1')

        # disable toxiproxy to kill connections
        toxiproxy.disable()

        # re-enable toxiproxy just before the consumer attempts to reconnect;
        # (consumer.cancel is the only hook we have)
        def reconnect(args, kwargs, res, exc_info):
            block.send(True)
            toxiproxy.enable()
            return True

        with wait_for_call(
            toxic_rpc_proxy._reply_listener.queue_consumer.consumer, 'cancel',
            callback=reconnect
        ):
            # rpc proxy should recover the message in flight
            res.result() == "msg1"

            # proxy should work again after reconnection
            assert toxic_rpc_proxy.service.method("msg2") == "msg2"

    def test_disconnect_and_fail_to_reconnect(
        self, container_factory, rabbit_manager, rabbit_config,
        toxic_rpc_proxy, toxiproxy
    ):
        block = Event()

        class Service(object):
            name = "service"

            @rpc
            def method(self, arg):
                block.wait()
                return arg

        container = container_factory(Service, rabbit_config)
        container.start()

        # make an async call that will block,
        # wait for the worker to have spawned
        with wait_for_call(container, 'spawn_worker'):
            res = toxic_rpc_proxy.service.method.call_async('msg1')

        # disable toxiproxy to kill connections
        with toxiproxy.disabled():

            # toxiproxy remains disabled when the proxy attempts to reconnect,
            # so we should return an error for the request in flight
            with pytest.raises(socket.error):
                res.result()

        # unblock worker
        block.send(True)

        # proxy will not work afterwards because the queueconsumer connection
        # was not recovered on the second attempt
        with pytest.raises(RuntimeError):
            toxic_rpc_proxy.service.method("msg2")


class TestConsumeEvent(object):

    @pytest.fixture
    def queue_consumer(self):
        queue_consumer = Mock()
        queue_consumer.stopped = False
        queue_consumer.connection.connected = True
        return queue_consumer

    def test_wait(self, queue_consumer):
        correlation_id = 1
        event = ConsumeEvent(queue_consumer, correlation_id)

        result = "result"

        def get_message(correlation_id):
            event.send(result)
        queue_consumer.get_message.side_effect = get_message

        assert event.wait() == result
        assert queue_consumer.get_message.call_args == call(correlation_id)

    def test_wait_exception_while_waiting(self, queue_consumer):
        correlation_id = 1
        event = ConsumeEvent(queue_consumer, correlation_id)

        exc = ExampleError()

        def get_message(correlation_id):
            event.send_exception(exc)
        queue_consumer.get_message.side_effect = get_message

        with pytest.raises(ExampleError):
            event.wait()
        assert queue_consumer.get_message.call_args == call(correlation_id)

    def test_wait_exception_before_wait(self, queue_consumer):
        correlation_id = 1
        event = ConsumeEvent(queue_consumer, correlation_id)

        exc = ExampleError()

        event.send_exception(exc)
        with pytest.raises(ExampleError):
            event.wait()
        assert not queue_consumer.get_message.called

    def test_wait_queue_consumer_stopped(self, queue_consumer):
        correlation_id = 1
        event = ConsumeEvent(queue_consumer, correlation_id)

        queue_consumer.stopped = True

        with pytest.raises(RuntimeError) as raised:
            event.wait()
        assert "stopped" in str(raised.value)
        assert not queue_consumer.get_message.called

    def test_wait_queue_consumer_disconnected(self, queue_consumer):
        correlation_id = 1
        event = ConsumeEvent(queue_consumer, correlation_id)

        queue_consumer.connection.connected = False

        with pytest.raises(RuntimeError) as raised:
            event.wait()
        assert "disconnected" in str(raised.value)
        assert not queue_consumer.get_message.called


def test_timeout_not_needed(container_factory, rabbit_manager, rabbit_config):
    container = container_factory(FooService, rabbit_config)
    container.start()

    with ServiceRpcProxy('foobar', rabbit_config, timeout=1) as proxy:
        assert proxy.sleep() == 0


def test_timeout(container_factory, rabbit_manager, rabbit_config):
    container = container_factory(FooService, rabbit_config)
    container.start()

    with ServiceRpcProxy('foobar', rabbit_config, timeout=.5) as proxy:
        with pytest.raises(RpcTimeout):
            proxy.sleep(seconds=1)

        # make sure we can still use the proxy
        assert proxy.sleep(seconds=0) == 0


def test_no_timeout(
    container_factory, rabbit_manager, rabbit_config
):
    container = container_factory(FooService, rabbit_config)
    container.start()

    with ServiceRpcProxy('foobar', rabbit_config) as proxy:
        with pytest.raises(eventlet.Timeout):
            with eventlet.Timeout(.1):
                proxy.sleep(seconds=1)


def test_async_timeout(
    container_factory, rabbit_manager, rabbit_config
):
    container = container_factory(FooService, rabbit_config)
    container.start()

    with ServiceRpcProxy('foobar', rabbit_config, timeout=.5) as proxy:
        result = proxy.sleep.call_async(seconds=1)
        with pytest.raises(RpcTimeout):
            result.result()

        result = proxy.sleep.call_async(seconds=.2)
        eventlet.sleep(.2)
        result.result()


def test_use_after_close(container_factory, rabbit_manager, rabbit_config):
    container = container_factory(FooService, rabbit_config)
    container.start()

    with ServiceRpcProxy('foobar', rabbit_config) as proxy:
        proxy.spam(ham=1)
        pass

    with pytest.raises(RuntimeError) as exc:
        proxy.spam(ham=1)
    assert 'can no longer be used' in str(exc)


@patch('nameko.rpc.RPC_REPLY_QUEUE_TTL', new=100)
def test_proxy_queue_expired_even_if_unused(rabbit_manager, rabbit_config):
    vhost = rabbit_config['vhost']
    with ServiceRpcProxy('exampleservice', rabbit_config):
        assert len(rabbit_manager.get_queues(vhost)) == 1

    eventlet.sleep(.15)  # sleep for >TTL
    assert len(rabbit_manager.get_queues(vhost)) == 0


def test_cluster_proxy(container_factory, rabbit_manager, rabbit_config):
    container = container_factory(FooService, rabbit_config)
    container.start()

    with ClusterRpcProxy(rabbit_config) as proxy:
        assert proxy.foobar.spam(ham=1) == 1


def test_cluster_proxy_reuse(container_factory, rabbit_manager, rabbit_config):
    container = container_factory(FooService, rabbit_config)
    container.start()

    cluster_proxy = ClusterRpcProxy(rabbit_config)
    with cluster_proxy as proxy:
        assert proxy.foobar.spam(ham=1) == 1

    with cluster_proxy as second_proxy:
        assert second_proxy.foobar.spam(ham=1) == 1


def test_cluster_proxy_dict_access(
    container_factory, rabbit_manager, rabbit_config
):
    container = container_factory(FooService, rabbit_config)
    container.start()

    with ClusterRpcProxy(rabbit_config) as proxy:
        assert proxy['foobar'].spam(ham=3) == 3


def test_recover_from_keyboardinterrupt(
    container_factory, rabbit_manager, rabbit_config
):
    container = container_factory(FooService, rabbit_config)
    container.start()  # create rpc queues
    container.stop()  # but make sure call doesn't complete

    with ServiceRpcProxy('foobar', rabbit_config) as proxy:

        with patch('kombu.connection.Connection.drain_events') as drain_events:
            drain_events.side_effect = KeyboardInterrupt('killing from test')
            with pytest.raises(KeyboardInterrupt):
                proxy.spam(ham=0)

        container = container_factory(FooService, rabbit_config)
        container.start()

        # proxy should still work
        assert proxy.spam(ham=1) == 1


def test_consumer_replacing(container_factory, rabbit_manager, rabbit_config):
    container = container_factory(FooService, rabbit_config)
    container.start()

    class FakeRepliesDict(dict):
        # act like the internal replies dict, but keep a list of messages
        # passing through for later inspection
        def __init__(self):
            self.messages = []

        def __setitem__(self, key, value):
            self.messages.append(value)
            super(FakeRepliesDict, self).__setitem__(key, value)

    fake_replies = FakeRepliesDict()

    with ServiceRpcProxy('foobar', rabbit_config) as proxy:
        # extra setup, as after e.g. connection error
        proxy.reply_listener.queue_consumer._setup_consumer()

        with patch.object(
            proxy.reply_listener.queue_consumer,
            'replies',
            fake_replies
        ):
            count = 10
            replies = [proxy.spam.call_async('hello') for _ in range(count)]
            assert [reply.result() for reply in replies] == ['hello'] * count

    consumer_tags = set()
    # there should only be a single consumer. we check by looking at the
    # consumer tag on the received messages
    for _, message in fake_replies.messages:
        consumer_tags.add(message.delivery_info['consumer_tag'])
    assert len(consumer_tags) == 1


@skip_if_no_toxiproxy
@pytest.mark.filterwarnings("ignore:Mandatory delivery:UserWarning")
class TestStandaloneProxyDisconnections(object):

    @pytest.fixture(autouse=True)
    def container(self, container_factory, rabbit_config):

        class Service(object):
            name = "service"

            @rpc
            def echo(self, arg):
                return arg

        config = rabbit_config

        container = container_factory(Service, config)
        container.start()

    @pytest.yield_fixture(autouse=True)
    def retry(self, request):
        retry = False
        if "publish_retry" in request.keywords:
            retry = True

        with patch.object(MethodProxy.publisher_cls, 'retry', new=retry):
            yield

    @pytest.yield_fixture(params=[True, False])
    def use_confirms(self, request):
        with patch.object(
            MethodProxy.publisher_cls, 'use_confirms', new=request.param
        ):
            yield request.param

    @pytest.yield_fixture(autouse=True)
    def toxic_rpc_proxy(self, toxiproxy):
        with patch.object(MethodProxy, 'amqp_uri', new=toxiproxy.uri):
            yield

    @pytest.yield_fixture
    def service_rpc(self, rabbit_config):
        with ServiceRpcProxy("service", rabbit_config) as proxy:
            yield proxy

    @pytest.mark.usefixtures('use_confirms')
    def test_normal(self, service_rpc):
        assert service_rpc.echo(1) == 1
        assert service_rpc.echo(2) == 2

    @pytest.mark.usefixtures('use_confirms')
    def test_down(self, service_rpc, toxiproxy):
        toxiproxy.disable()

        with pytest.raises(OperationalError) as exc_info:
            service_rpc.echo(1)
        assert "ECONNREFUSED" in str(exc_info.value)

    @pytest.mark.usefixtures('use_confirms')
    def test_timeout(self, service_rpc, toxiproxy):
        toxiproxy.set_timeout()

        with pytest.raises(OperationalError):
            service_rpc.echo(1)

    def test_reuse_when_down(self, service_rpc, toxiproxy):
        """ Verify we detect stale connections.

        Publish confirms are required for this functionality. Without confirms
        the later messages are silently lost and the test hangs waiting for a
        response.
        """
        assert service_rpc.echo(1) == 1

        toxiproxy.disable()

        # publisher cannot connect, raises
        with pytest.raises(IOError):
            service_rpc.echo(2)

    def test_reuse_when_recovered(self, service_rpc, toxiproxy):
        """ Verify we detect and recover from stale connections.

        Publish confirms are required for this functionality. Without confirms
        the later messages are silently lost and the test hangs waiting for a
        response.
        """
        assert service_rpc.echo(1) == 1

        toxiproxy.disable()

        # publisher cannot connect, raises
        with pytest.raises(IOError):
            service_rpc.echo(2)

        toxiproxy.enable()

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

        # subsequent calls succeed (after reconnecting via retry policy)
        with wait_for_call(
            Connection,
            "_establish_connection",
            callback=enable_after_retry
        ):
            assert service_rpc.echo(2) == 2


@skip_if_no_toxiproxy
class TestStandaloneProxyConsumerDisconnections(object):

    @pytest.fixture(autouse=True)
    def container(self, container_factory, rabbit_config):

        class Service(object):
            name = "service"

            @rpc
            def echo(self, arg):
                print("ECHO!", arg)
                return arg

        config = rabbit_config

        container = container_factory(Service, config)
        container.start()

    @pytest.yield_fixture(autouse=True)
    def non_toxic_rpc_proxy(self, rabbit_config):
        """ Fix the AMQP URI passes to the publisher so we're only testing
        the effect of the broken connection on the consumer.
        """
        amqp_uri = rabbit_config['AMQP_URI']
        with patch.object(MethodProxy, 'amqp_uri', new=amqp_uri):
            yield

    @pytest.yield_fixture
    def service_rpc(self, toxiproxy, rabbit_config):

        config = rabbit_config.copy()
        config['AMQP_URI'] = toxiproxy.uri

        with ServiceRpcProxy("service", config) as proxy:
            yield proxy

    def test_normal(self, service_rpc):
        assert service_rpc.echo(1) == 1
        assert service_rpc.echo(2) == 2

    def test_down(self, service_rpc, toxiproxy):
        with toxiproxy.disabled():

            # fails to set up initial consumer
            with pytest.raises(socket.error) as exc_info:
                service_rpc.echo(1)
            assert "ECONNREFUSED" in str(exc_info.value)

    def test_timeout(self, service_rpc, toxiproxy):
        with toxiproxy.timeout(stream="downstream"):

            with pytest.raises(IOError):
                service_rpc.echo(1)

    def test_reuse_when_down(self, service_rpc, toxiproxy):
        assert service_rpc.echo(1) == 1

        with toxiproxy.disabled():

            with pytest.raises(socket.error) as exc_info:
                service_rpc.echo(2)
            assert "ECONNREFUSED" in str(exc_info.value)

    def test_reuse_when_recovered(self, service_rpc, toxiproxy):
        assert service_rpc.echo(1) == 1

        with toxiproxy.disabled():

            with pytest.raises(socket.error) as exc_info:
                service_rpc.echo(2)
            assert "ECONNREFUSED" in str(exc_info.value)

        # can't reuse it
        with pytest.raises(RuntimeError) as raised:
            service_rpc.echo(3)
        assert "This consumer has been disconnected" in str(raised.value)


class TestSSL(object):

    def test_rpc_proxy_over_ssl(
        self, container_factory, rabbit_ssl_config, rabbit_config
    ):
        class Service(object):
            name = "service"

            @rpc
            def echo(self, *args, **kwargs):
                return args, kwargs

        container = container_factory(Service, rabbit_config)
        container.start()

        with ServiceRpcProxy("service", rabbit_ssl_config) as proxy:
            assert proxy.echo("a", "b", foo="bar") == [
                ['a', 'b'], {'foo': 'bar'}
            ]
