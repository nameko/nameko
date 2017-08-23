import itertools
import socket

import eventlet
import pytest
from eventlet.event import Event
from kombu.connection import Connection
from kombu.message import Message

from nameko.containers import WorkerContext
from nameko.exceptions import RemoteError, RpcConnectionError, RpcTimeout
from nameko.extensions import DependencyProvider
from nameko.rpc import MethodProxy, Responder, get_rpc_exchange, rpc
from nameko.standalone.rpc import ClusterRpcProxy, ServiceRpcProxy
from nameko.testing.utils import get_rabbit_connections
from nameko.testing.waiting import wait_for_call
from nameko.utils.retry import retry

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


def test_reply_queue_autodelete(
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
        assert foo.spam(ham='eggs') == 'eggs'

    queues_after = list_queues()
    assert queues_after == queues_before

    # check proxy re-use
    with ServiceRpcProxy('foobar', rabbit_config) as foo:
        assert foo.spam(ham='eggs') == 'eggs'
        assert foo.spam(ham='eggs') == 'eggs'

    queues_after = list_queues()
    assert queues_after == queues_before


def test_unexpected_correlation_id(container_factory, rabbit_config):
    container = container_factory(FooService, rabbit_config)
    container.start()

    with ServiceRpcProxy("foobar", rabbit_config) as proxy:

        message = Message(channel=None, properties={
            'reply_to': proxy.reply_listener.routing_key,
            'correlation_id': 'invalid',
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


def test_disconnect_with_pending_reply(
    container_factory, rabbit_manager, rabbit_config
):
    block = Event()

    class ExampleService(object):
        name = "exampleservice"

        def hook(self):
            pass  # pragma: no cover

        @rpc
        def method(self, arg):
            self.hook()
            block.wait()
            return arg

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    vhost = rabbit_config['vhost']

    # get exampleservice's queue consumer connection while we know it's the
    # only active connection
    connections = get_rabbit_connections(vhost, rabbit_manager)
    assert len(connections) == 1
    container_connection = connections[0]

    with ServiceRpcProxy('exampleservice', rabbit_config) as proxy:

        # grab the proxy's connection too, the only other connection
        connections = get_rabbit_connections(vhost, rabbit_manager)
        assert len(connections) == 2
        proxy_connection = [
            conn for conn in connections if conn != container_connection
        ][0]

        counter = itertools.count(start=1)

        class ConnectionStillOpen(Exception):
            pass

        @retry(for_exceptions=ConnectionStillOpen, delay=0.2)
        def wait_for_connection_close(name):
            connections = get_rabbit_connections(vhost, rabbit_manager)
            for conn in connections:
                if conn['name'] == name:
                    raise ConnectionStillOpen(name)  # pragma: no cover

        def cb(args, kwargs, res, exc_info):
            # trigger a disconnection on the second call.
            # release running workers once the connection has been closed
            count = next(counter)
            if count == 2:
                rabbit_manager.delete_connection(proxy_connection['name'])
                wait_for_connection_close(proxy_connection['name'])
                block.send(True)
                return True

        # attach a callback to `hook` so we can close the connection
        # while there are requests in-flight
        with wait_for_call(ExampleService, 'hook', callback=cb):

            # make an async call that runs for some time
            async_call = proxy.method.call_async("hello")

            # make another call that will trigger the disconnection;
            # expect the blocking proxy to raise when the service reconnects
            with pytest.raises(RpcConnectionError):
                proxy.method("hello")

            # also expect the running call to raise, since the reply may have
            # been sent while the queue was gone (deleted on disconnect, and
            # not added until re-connect)
            with pytest.raises(RpcConnectionError):
                async_call.result()

        # proxy should work again afterwards
        assert proxy.method("hello") == "hello"


def test_timeout_not_needed(container_factory, rabbit_manager, rabbit_config):
    container = container_factory(FooService, rabbit_config)
    container.start()

    with ServiceRpcProxy('foobar', rabbit_config, timeout=1) as proxy:
        assert proxy.sleep() == 0


def test_timeout(container_factory, rabbit_manager, rabbit_config):
    container = container_factory(FooService, rabbit_config)
    container.start()

    with ServiceRpcProxy('foobar', rabbit_config, timeout=.1) as proxy:
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

    with ServiceRpcProxy('foobar', rabbit_config, timeout=.1) as proxy:
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


def test_proxy_deletes_queue_even_if_unused(rabbit_manager, rabbit_config):
    vhost = rabbit_config['vhost']
    with ServiceRpcProxy('exampleservice', rabbit_config):
        assert len(rabbit_manager.get_queues(vhost)) == 1

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

        with pytest.raises(socket.error) as exc_info:
            service_rpc.echo(1)
        assert "ECONNREFUSED" in str(exc_info.value)

    @pytest.mark.usefixtures('use_confirms')
    def test_timeout(self, service_rpc, toxiproxy):
        toxiproxy.set_timeout()

        with pytest.raises(IOError) as exc_info:
            service_rpc.echo(1)
        assert "Socket closed" in str(exc_info.value)

    def test_reuse_when_down(self, service_rpc, toxiproxy):
        """ Verify we detect stale connections.

        Publish confirms are required for this functionality. Without confirms
        the later messages are silently lost and the test hangs waiting for a
        response.
        """
        assert service_rpc.echo(1) == 1

        toxiproxy.disable()

        # publisher cannot connect, raises
        with pytest.raises(IOError) as exc_info:
            service_rpc.echo(2)
        assert (
            # expect the write to raise a BrokenPipe or, if it succeeds,
            # the socket to be closed on the subsequent confirmation read
            "Broken pipe" in str(exc_info.value) or
            "Socket closed" in str(exc_info.value)
        )

    def test_reuse_when_recovered(self, service_rpc, toxiproxy):
        """ Verify we detect and recover from stale connections.

        Publish confirms are required for this functionality. Without confirms
        the later messages are silently lost and the test hangs waiting for a
        response.
        """
        assert service_rpc.echo(1) == 1

        toxiproxy.disable()

        # publisher cannot connect, raises
        with pytest.raises(IOError) as exc_info:
            service_rpc.echo(2)
        assert (
            # expect the write to raise a BrokenPipe or, if it succeeds,
            # the socket to be closed on the subsequent confirmation read
            "Broken pipe" in str(exc_info.value) or
            "Socket closed" in str(exc_info.value)
        )

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
        with wait_for_call(Connection, 'connect', callback=enable_after_retry):
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
        toxiproxy.disable()

        # fails to set up initial consumer
        with pytest.raises(socket.error) as exc_info:
            service_rpc.echo(1)
        assert "ECONNREFUSED" in str(exc_info.value)

    def test_timeout(self, service_rpc, toxiproxy):
        toxiproxy.set_timeout(stream="downstream")

        with pytest.raises(IOError) as exc_info:
            service_rpc.echo(1)
        assert "Socket closed" in str(exc_info.value)

    def test_reuse_when_down(self, service_rpc, toxiproxy):
        assert service_rpc.echo(1) == 1

        toxiproxy.disable()

        with pytest.raises(socket.error) as exc_info:
            service_rpc.echo(2)
        assert "ECONNREFUSED" in str(exc_info.value)

    def test_reuse_when_recovered(self, service_rpc, toxiproxy):
        assert service_rpc.echo(1) == 1

        toxiproxy.disable()

        with pytest.raises(socket.error) as exc_info:
            service_rpc.echo(2)
        assert "ECONNREFUSED" in str(exc_info.value)

        toxiproxy.enable()

        # can't reuse it
        with pytest.raises(RuntimeError) as raised:
            service_rpc.echo(3)
        assert "This consumer has been disconnected" in str(raised.value)
