import socket
import uuid
from contextlib import contextmanager

import eventlet
import pytest
from eventlet.event import Event
from kombu.connection import Connection
from kombu.message import Message
from mock import Mock, patch
from six.moves import queue

from nameko.constants import HEARTBEAT_CONFIG_KEY
from nameko.containers import WorkerContext
from nameko.exceptions import (
    RemoteError, ReplyQueueExpiredWithPendingReplies, RpcTimeout
)
from nameko.extensions import DependencyProvider
from nameko.rpc import Responder, get_rpc_exchange, rpc
from nameko.standalone.rpc import (
    ClusterRpcProxy, ReplyListener, RpcProxy, ServiceRpcProxy
)
from nameko.testing.waiting import wait_for_call

from test import skip_if_no_toxiproxy


class ContextReader(DependencyProvider):
    """ Access values from the worker context data.

    This is a test facilty! Write specific Dependencies to make use of
    values in ``WorkerContext.data``, don't expose it directly.
    """

    def get_dependency(self, worker_ctx):
        def get_context_value(key):
            return worker_ctx.context_data.get(key)
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


def test_proxy_stop_start_again(container_factory, rabbit_config):

    container = container_factory(FooService, rabbit_config)
    container.start()

    foobar_proxy = ServiceRpcProxy('foobar', rabbit_config)
    foo1 = foobar_proxy.start()
    assert foo1.spam(ham='eggs') == 'eggs'
    foobar_proxy.stop()

    foo2 = foobar_proxy.start()
    assert foo2.spam(ham='eggs') == 'eggs'  # test re-use
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


@pytest.mark.usefixtures('predictable_call_ids')
def test_call_id(container_factory, rabbit_config):

    container = container_factory(FooService, rabbit_config)
    container.start()

    with ServiceRpcProxy('foobar', rabbit_config) as foo:
        stack1 = foo.get_context_data('call_id_stack')
        assert stack1 == [
            'standalone_rpc_proxy.0.0',
            'foobar.get_context_data.1'
        ]
        stack2 = foo.get_context_data('call_id_stack')
        assert stack2 == [
            'standalone_rpc_proxy.0.2',
            'foobar.get_context_data.3'
        ]


def test_proxy_remote_error(container_factory, rabbit_config):

    container = container_factory(FooService, rabbit_config)
    container.start()

    with ServiceRpcProxy("foobar", rabbit_config) as proxy:
        with pytest.raises(RemoteError) as exc_info:
            proxy.broken()
        assert exc_info.value.exc_type == "ExampleError"


@patch('nameko.standalone.rpc.RPC_REPLY_QUEUE_TTL', new=200)
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

    eventlet.sleep(.3)  # sleep for >TTL
    queues_after = list_queues()

    assert queues_before != queues_during
    assert queues_after == queues_before

    # check proxy re-use
    with ServiceRpcProxy('foobar', rabbit_config) as foo:
        assert foo.spam(ham='eggs') == 'eggs'
        assert foo.spam(ham='eggs') == 'eggs'

    eventlet.sleep(.3)  # sleep for >TTL
    queues_after = list_queues()
    assert queues_after == queues_before


@patch('nameko.standalone.rpc.RPC_REPLY_QUEUE_TTL', new=200)
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
        assert foo.sleep(0.4) == 0.4
        queues_between = list_queues()
        assert foo.spam(ham='eggs') == 'eggs'
        queues_after = list_queues()

    assert queues_before == queues_between == queues_after


@skip_if_no_toxiproxy
class TestDisconnectedWhileWaitingForReply(object):

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
            ReplyListener, 'establish_connection', new=establish_connection
        ):
            yield

    @pytest.yield_fixture(autouse=True)
    def fast_expiry(self):
        with patch('nameko.standalone.rpc.RPC_REPLY_QUEUE_TTL', new=100):
            yield

    @pytest.fixture(autouse=True)
    def container(
        self, container_factory, rabbit_config, rabbit_manager, toxiproxy,
        fast_expiry
    ):

        def enable_after_queue_expires():
            eventlet.sleep(1)
            toxiproxy.enable()

        class Service(object):
            name = "service"

            @rpc
            def sleep(self):
                toxiproxy.disable()
                eventlet.spawn_n(enable_after_queue_expires)
                return "OK"

        config = rabbit_config.copy()
        container = container_factory(Service, config)
        container.start()

        return container

    @pytest.yield_fixture
    def toxic_rpc_proxy(self, rabbit_config, toxiproxy):
        config = rabbit_config.copy()
        config['AMQP_URI'] = toxiproxy.uri
        with ClusterRpcProxy(config) as proxy:
            yield proxy

    def test_reply_queue_removed_while_disconnected_with_pending_reply(
        self, toxic_rpc_proxy, toxiproxy, container
    ):
        with pytest.raises(ReplyQueueExpiredWithPendingReplies):
            toxic_rpc_proxy.service.sleep()


@patch('nameko.standalone.rpc.RPC_REPLY_QUEUE_TTL', new=200)
def test_async_wait_longer_than_expiry(container_factory, rabbit_config):
    """ Replies to async requests will be lost if the reply queue expiry is
    shorter than the period waited before consuming the result.
    """

    container = container_factory(FooService, rabbit_config)
    container.start()

    with ServiceRpcProxy('foobar', rabbit_config, timeout=0.6) as foo:
        res = foo.sleep.call_async(0.4)
        eventlet.sleep(0.4)

        with pytest.raises(ReplyQueueExpiredWithPendingReplies):
            res.result()


@patch('nameko.standalone.rpc.RPC_REPLY_QUEUE_TTL', new=200)
def test_request_longer_than_expiry(container_factory, rabbit_config):
    container = container_factory(FooService, rabbit_config)
    container.start()

    with ServiceRpcProxy('foobar', rabbit_config) as foo:
        assert foo.sleep(0.4) == 0.4


def test_unexpected_correlation_id(container_factory, rabbit_config):
    container = container_factory(FooService, rabbit_config)
    container.start()

    service_rpc_proxy = ServiceRpcProxy("foobar", rabbit_config)
    with service_rpc_proxy as proxy:

        message = Message(channel=None, properties={
            'reply_to': service_rpc_proxy.reply_listener.routing_key,
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

    @pytest.fixture
    def toxic_rpc_proxy(self, rabbit_config, toxiproxy):
        rabbit_config['AMQP_URI'] = toxiproxy.uri
        return ClusterRpcProxy(rabbit_config)

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

        with toxic_rpc_proxy as proxy:

            # make an async call that will block,
            # wait for the worker to have spawned
            with wait_for_call(container, 'spawn_worker'):
                res = proxy.service.method.call_async('msg1')

            # disable toxiproxy to kill connections
            toxiproxy.disable()

            # re-enable toxiproxy when the connection error is detected
            def reconnect(args, kwargs, res, exc_info):
                block.send(True)
                toxiproxy.enable()
                return True

            with wait_for_call(
                toxic_rpc_proxy.reply_listener, 'on_connection_error',
                callback=reconnect
            ):
                # rpc proxy should recover the message in flight
                res.result() == "msg1"

                # proxy should work again after reconnection
                assert proxy.service.method("msg2") == "msg2"

            # stop container before we stop toxiproxy
            container.stop()


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
            proxy.sleep(seconds=2)

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
        result = proxy.sleep.call_async(seconds=2)
        with pytest.raises(RpcTimeout):
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


@patch('nameko.standalone.rpc.RPC_REPLY_QUEUE_TTL', new=100)
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


class TestConfigurability(object):
    """
    Test and demonstrate configuration options for the standalone RPC proxy
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
        config = {'AMQP_URI': 'memory://localhost'}

        value = Mock()

        rpc_proxy = RpcProxy(
            config, **{parameter: value}
        )
        with rpc_proxy as proxy:
            proxy.service.method.call_async()
        assert producer.publish.call_args[1][parameter] == value

    @pytest.mark.usefixtures('predictable_call_ids')
    def test_headers(self, mock_container, producer):
        """ Headers can be provided at instantiation time, and are merged with
        Nameko headers.
        """
        config = {'AMQP_URI': 'memory://localhost'}

        data = {'context': 'data'}

        value = {'foo': Mock()}

        rpc_proxy = RpcProxy(
            config, context_data=data, **{'headers': value}
        )

        with rpc_proxy as proxy:
            proxy['service-name'].method.call_async()

        def merge_dicts(base, *updates):
            merged = base.copy()
            [merged.update(update) for update in updates]
            return merged

        nameko_headers = {
            'nameko.context': 'data',
            'nameko.call_id_stack': ['standalone_rpc_proxy.0.0'],
        }

        assert producer.publish.call_args[1]['headers'] == merge_dicts(
            nameko_headers, value
        )

    @patch('nameko.standalone.rpc.uuid')
    def test_restricted_parameters(
        self, patch_uuid, mock_container, producer
    ):
        """ Verify that providing routing parameters at instantiation
        time has no effect.
        """
        config = {'AMQP_URI': 'memory://localhost'}

        uuid1 = uuid.uuid4()
        uuid2 = uuid.uuid4()
        patch_uuid.uuid4.side_effect = [uuid1, uuid2]

        restricted_params = (
            'exchange', 'routing_key', 'mandatory',
            'correlation_id', 'reply_to'
        )

        rpc_proxy = RpcProxy(
            config, **{param: Mock() for param in restricted_params}
        )

        with rpc_proxy as proxy:
            proxy.service.method.call_async()

        publish_params = producer.publish.call_args[1]

        assert publish_params['exchange'].name == "nameko-rpc"
        assert publish_params['routing_key'] == 'service.method'
        assert publish_params['mandatory'] is True
        assert publish_params['reply_to'] == str(uuid1)
        assert publish_params['correlation_id'] == str(uuid2)


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

        with patch.object(
            RpcProxy.publisher_cls, 'retry', new=retry
        ):
            yield

    @pytest.yield_fixture(params=[True, False])
    def use_confirms(self, request):
        with patch.object(
            RpcProxy.publisher_cls, 'use_confirms',
            new=request.param
        ):
            yield request.param

    @pytest.yield_fixture(autouse=True)
    def toxic_rpc_proxy(self, toxiproxy):
        with patch.object(RpcProxy, 'amqp_uri', new=toxiproxy.uri):
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
class TestStandaloneProxyReplyListenerDisconnections(object):

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
            ReplyListener, 'establish_connection', new=establish_connection
        ):
            yield

    @pytest.fixture(autouse=True)
    def container(self, container_factory, rabbit_config):

        class Service(object):
            name = "service"

            @rpc
            def echo(self, arg):
                return arg

        config = rabbit_config

        # very fast heartbeat
        config = rabbit_config
        config[HEARTBEAT_CONFIG_KEY] = 2  # seconds

        container = container_factory(Service, config)
        container.start()

    @pytest.yield_fixture(autouse=True)
    def toxic_reply_listener(self, toxiproxy):
        with patch.object(ReplyListener, 'amqp_uri', new=toxiproxy.uri):
            yield

    @pytest.fixture
    def rpc_proxy(self, rabbit_config):
        return ServiceRpcProxy('service', rabbit_config)

    @pytest.fixture
    def reply_listener(self, rpc_proxy):
        return rpc_proxy.reply_listener

    @pytest.yield_fixture
    def service_rpc(self, rpc_proxy):
        with rpc_proxy as proxy:
            yield proxy

    def test_normal(self, container, service_rpc):
        assert service_rpc.echo("foo") == "foo"

    def test_down(self, reply_listener, service_rpc, toxiproxy):
        """ Verify we detect and recover from closed sockets.

        This failure mode closes the socket between the consumer and the
        rabbit broker.

        Attempting to read from the closed socket raises a socket.error
        and the connection is re-established.
        """
        def reset(args, kwargs, result, exc_info):
            toxiproxy.enable()
            return True

        with wait_for_call(
            reply_listener, 'on_connection_error', callback=reset
        ):
            toxiproxy.disable()

            # connection re-established after one error;
            # make the request _inside_ the context block because the reply
            # listener doesn't establish a connection until consuming a result
            assert service_rpc.echo("foo") == "foo"

    def test_upstream_timeout(self, reply_listener, service_rpc, toxiproxy):
        """ Verify we detect and recover from sockets timing out.

        This failure mode means that the socket between the consumer and the
        rabbit broker times for out `timeout` milliseconds and then closes.

        Attempting to read from the socket after it's closed raises a
        socket.error and the connection will be re-established. If `timeout`
        is longer than twice the heartbeat interval, the behaviour is the same
        as in `test_upstream_blackhole` below.
        """
        def reset(args, kwargs, result, exc_info):
            toxiproxy.reset_timeout()
            return True

        with wait_for_call(
            reply_listener, 'on_connection_error', callback=reset
        ):
            toxiproxy.set_timeout(timeout=100)

            # connection re-established after one error;
            # make the request _inside_ the context block because the reply
            # listener doesn't establish a connection until consuming a result
            assert service_rpc.echo("foo") == "foo"

    def test_upstream_blackhole(self, reply_listener, service_rpc, toxiproxy):
        """ Verify we detect and recover from sockets losing data.

        This failure mode means that all data sent from the consumer to the
        rabbit broker is lost, but the socket remains open.

        Heartbeats sent from the consumer are not received by the broker. After
        two beats are missed the broker closes the connection, and subsequent
        reads from the socket raise a socket.error, so the connection is
        re-established.
        """
        def reset(args, kwargs, result, exc_info):
            toxiproxy.reset_timeout()
            return True

        with wait_for_call(
            reply_listener, 'on_connection_error', callback=reset
        ):
            toxiproxy.set_timeout(timeout=0)

            # connection re-established after one error;
            # make the request _inside_ the context block because the reply
            # listener doesn't establish a connection until consuming a result
            assert service_rpc.echo("foo") == "foo"

    def test_downstream_timeout(self, reply_listener, service_rpc, toxiproxy):
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
        def reset(args, kwargs, result, exc_info):
            toxiproxy.reset_timeout()
            return True

        with wait_for_call(
            reply_listener, 'on_connection_error', callback=reset
        ):
            toxiproxy.set_timeout(stream="downstream", timeout=100)

            # connection re-established after one error;
            # make the request _inside_ the context block because the reply
            # listener doesn't establish a connection until consuming a result
            assert service_rpc.echo("foo") == "foo"

    def test_downstream_blackhole(
        self, reply_listener, service_rpc, toxiproxy
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

        def reset(args, kwargs, result, exc_info):
            toxiproxy.reset_timeout()
            return True

        with wait_for_call(
            reply_listener, 'on_connection_error', callback=reset
        ):
            toxiproxy.set_timeout(stream="downstream", timeout=0)

            # connection re-established after one error;
            # make the request _inside_ the context block because the reply
            # listener doesn't establish a connection until consuming a result
            assert service_rpc.echo("foo") == "foo"
