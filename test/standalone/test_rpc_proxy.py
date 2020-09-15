from contextlib import contextmanager

import eventlet
import pytest
from eventlet.event import Event
from kombu.connection import Connection
from kombu.exceptions import OperationalError
from kombu.message import Message
from mock import Mock, patch
from six.moves import queue

from nameko import config
from nameko.amqp.consume import Consumer
from nameko.constants import HEARTBEAT_CONFIG_KEY
from nameko.containers import WorkerContext
from nameko.exceptions import (
    RemoteError, ReplyQueueExpiredWithPendingReplies, RpcTimeout
)
from nameko.extensions import DependencyProvider
from nameko.rpc import Responder, get_rpc_exchange, rpc
from nameko.standalone.rpc import (
    ClusterRpcClient, ReplyListener, ServiceRpcClient
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


@pytest.yield_fixture
def toxic_reply_listener(toxiproxy):
    class ToxicReplyListener(ReplyListener):
        def __init__(self, *args, **kwargs):
            kwargs["uri"] = toxiproxy.uri
            return super(ToxicReplyListener, self).__init__(*args, **kwargs)
    with patch('nameko.standalone.rpc.ReplyListener', ToxicReplyListener):
        yield


@pytest.mark.usefixtures('rabbit_config')
def test_client(container_factory):

    container = container_factory(FooService)
    container.start()

    with ServiceRpcClient('foobar') as foo:
        assert foo.spam(ham='eggs') == 'eggs'
        assert foo.spam(ham='eggs') == 'eggs'  # test re-use


@pytest.mark.usefixtures('rabbit_config')
def test_client_manual_start_stop(container_factory):

    container = container_factory(FooService)
    container.start()

    foobar_client = ServiceRpcClient('foobar')
    foo = foobar_client.start()
    assert foo.spam(ham='eggs') == 'eggs'
    assert foo.spam(ham='eggs') == 'eggs'  # test re-use
    foobar_client.stop()


@pytest.mark.usefixtures('rabbit_config')
def test_client_stop_start_again(container_factory):

    container = container_factory(FooService)
    container.start()

    foobar_client = ServiceRpcClient('foobar')
    foo1 = foobar_client.start()
    assert foo1.spam(ham='eggs') == 'eggs'
    foobar_client.stop()

    foo2 = foobar_client.start()
    assert foo2.spam(ham='eggs') == 'eggs'  # test re-use
    foobar_client.stop()


@pytest.mark.usefixtures('rabbit_config')
def test_client_context_data(container_factory):

    container = container_factory(FooService)
    container.start()

    context_data = {'language': 'en'}
    with ServiceRpcClient('foobar', context_data) as foo:
        assert foo.get_context_data('language') == 'en'

    context_data = {'language': 'fr'}
    with ServiceRpcClient('foobar', context_data) as foo:
        assert foo.get_context_data('language') == 'fr'


@pytest.mark.usefixtures('rabbit_config')
@pytest.mark.usefixtures('predictable_call_ids')
def test_call_id(container_factory):

    container = container_factory(FooService)
    container.start()

    with ServiceRpcClient('foobar') as foo:
        stack1 = foo.get_context_data('call_id_stack')
        assert stack1 == [
            'standalone_rpc_client.0.0',
            'foobar.get_context_data.1'
        ]
        stack2 = foo.get_context_data('call_id_stack')
        assert stack2 == [
            'standalone_rpc_client.0.2',
            'foobar.get_context_data.3'
        ]


@pytest.mark.usefixtures('rabbit_config')
def test_client_remote_error(container_factory):

    container = container_factory(FooService)
    container.start()

    with ServiceRpcClient("foobar") as client:
        with pytest.raises(RemoteError) as exc_info:
            client.broken()
        assert exc_info.value.exc_type == "ExampleError"


@pytest.mark.usefixtures('rabbit_config')
@patch('nameko.standalone.rpc.RPC_REPLY_QUEUE_TTL', new=200)
def test_reply_queue_removed_on_expiry(
    rabbit_manager, get_vhost, container_factory
):
    def list_queues():
        vhost = get_vhost(config['AMQP_URI'])
        return [
            queue['name']
            for queue in rabbit_manager.get_queues(vhost=vhost)
        ]

    container = container_factory(FooService)
    container.start()

    queues_before = list_queues()

    with ServiceRpcClient('foobar') as foo:
        queues_during = list_queues()
        assert foo.spam(ham='eggs') == 'eggs'

    eventlet.sleep(.3)  # sleep for >TTL
    queues_after = list_queues()

    assert queues_before != queues_during
    assert queues_after == queues_before

    # check client re-use
    with ServiceRpcClient('foobar') as foo:
        assert foo.spam(ham='eggs') == 'eggs'
        assert foo.spam(ham='eggs') == 'eggs'

    eventlet.sleep(.3)  # sleep for >TTL
    queues_after = list_queues()
    assert queues_after == queues_before


@pytest.mark.usefixtures('rabbit_config')
@patch('nameko.standalone.rpc.RPC_REPLY_QUEUE_TTL', new=200)
def test_reply_queue_not_removed_while_in_use(
    rabbit_manager, get_vhost, container_factory
):
    def list_queues():
        vhost = get_vhost(config['AMQP_URI'])
        return [
            queue['name']
            for queue in rabbit_manager.get_queues(vhost=vhost)
        ]

    container = container_factory(FooService)
    container.start()

    # check client re-use
    with ServiceRpcClient('foobar') as foo:
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
            Consumer, 'establish_connection', new=establish_connection
        ):
            yield

    @pytest.yield_fixture(autouse=True)
    def fast_expiry(self):
        with patch('nameko.standalone.rpc.RPC_REPLY_QUEUE_TTL', new=100):
            yield

    @pytest.fixture(autouse=True)
    def container(
        self, container_factory, rabbit_manager, rabbit_config, toxiproxy,
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

        container = container_factory(Service)
        container.start()

        return container

    @pytest.mark.usefixtures('rabbit_config')
    def test_reply_queue_removed_while_disconnected_with_pending_reply(
        self, container, toxiproxy, toxic_reply_listener
    ):
        with pytest.raises(ReplyQueueExpiredWithPendingReplies):
            with ClusterRpcClient() as client:
                client.service.sleep()


@pytest.mark.usefixtures('rabbit_config')
@patch('nameko.standalone.rpc.RPC_REPLY_QUEUE_TTL', new=200)
def test_async_wait_longer_than_expiry(container_factory):
    """ Replies to async requests will be lost if the reply queue expiry is
    shorter than the period waited before consuming the result.
    """

    container = container_factory(FooService)
    container.start()

    with ServiceRpcClient('foobar', timeout=0.6) as foo:
        res = foo.sleep.call_async(0.4)
        eventlet.sleep(0.4)

        with pytest.raises(ReplyQueueExpiredWithPendingReplies):
            res.result()


@pytest.mark.usefixtures('rabbit_config')
@patch('nameko.standalone.rpc.RPC_REPLY_QUEUE_TTL', new=200)
def test_request_longer_than_expiry(container_factory):
    container = container_factory(FooService)
    container.start()

    with ServiceRpcClient('foobar') as foo:
        assert foo.sleep(0.4) == 0.4


@pytest.mark.usefixtures('rabbit_config')
@patch('nameko.standalone.rpc.RPC_REPLY_QUEUE_TTL', new=200)
def test_inactive_longer_than_expiry(container_factory):
    container = container_factory(FooService)
    container.start()

    with ServiceRpcClient('foobar') as foo:
        eventlet.sleep(0.4)
        assert foo.spam(ham='eggs') == 'eggs'


@pytest.mark.usefixtures("rabbit_config")
def test_unexpected_correlation_id(container_factory):
    container = container_factory(FooService)
    container.start()

    service_rpc_client = ServiceRpcClient("foobar")
    with service_rpc_client as client:

        message = Message(channel=None, properties={
            'reply_to': service_rpc_client.reply_listener.queue.routing_key,
            'correlation_id': 'invalid',
            'content_type': 'application/json'
        })
        amqp_uri = config['AMQP_URI']
        exchange = get_rpc_exchange()

        responder = Responder(amqp_uri, exchange, message)
        with patch('nameko.standalone.rpc._logger', autospec=True) as logger:
            responder.send_response(None, None)
            assert client.spam(ham='eggs') == 'eggs'
            assert logger.debug.call_count == 1


@pytest.mark.usefixtures('rabbit_config')
def test_async_rpc(container_factory):

    container = container_factory(FooService)
    container.start()

    with ServiceRpcClient('foobar') as foo:
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


@pytest.mark.usefixtures('rabbit_config')
def test_multiple_proxies(container_factory):
    container = container_factory(FooService)
    container.start()

    with ServiceRpcClient('foobar') as client1:
        res1 = client1.spam.call_async(ham=1)

        with ServiceRpcClient('foobar') as client2:
            res2 = client2.spam.call_async(ham=2)

            assert res1.result() == 1
            assert res2.result() == 2


@pytest.mark.usefixtures('rabbit_config')
def test_multiple_calls_to_result(container_factory):
    container = container_factory(FooService)
    container.start()

    with ServiceRpcClient('foobar') as client:
        res = client.spam.call_async(ham=1)
        res.result()
        res.result()


@skip_if_no_toxiproxy
class TestDisconnectWithPendingReply(object):

    @pytest.yield_fixture
    def toxic_rpc_client(self, rabbit_config, toxiproxy):
        with config.patch({'AMQP_URI': toxiproxy.uri}):
            yield ClusterRpcClient()

    def test_disconnect_and_successfully_reconnect(
        self, container_factory, rabbit_manager, toxic_rpc_client, toxiproxy
    ):
        block = Event()

        class Service(object):
            name = "service"

            @rpc
            def method(self, arg):
                block.wait()
                return arg

        container = container_factory(Service)
        container.start()

        with toxic_rpc_client as client:

            # make an async call that will block,
            # wait for the worker to have spawned
            with wait_for_call(container, 'spawn_worker'):
                res = client.service.method.call_async('msg1')

            # disable toxiproxy to kill connections
            toxiproxy.disable()

            # re-enable toxiproxy when the connection error is detected
            def reconnect(args, kwargs, res, exc_info):
                block.send(True)
                toxiproxy.enable()
                return True

            with wait_for_call(
                toxic_rpc_client.reply_listener.consumer,
                'on_connection_error',
                callback=reconnect
            ):
                # rpc client should recover the message in flight
                res.result() == "msg1"

                # client should work again after reconnection
                assert client.service.method("msg2") == "msg2"

            # stop container before we stop toxiproxy
            container.stop()


@pytest.mark.usefixtures('rabbit_config')
def test_timeout_not_needed(container_factory, rabbit_manager):
    container = container_factory(FooService)
    container.start()

    with ServiceRpcClient('foobar', timeout=1) as client:
        assert client.sleep() == 0


@pytest.mark.usefixtures('rabbit_config')
def test_timeout(container_factory, rabbit_manager):
    container = container_factory(FooService)
    container.start()

    with ServiceRpcClient('foobar', timeout=.5) as client:
        with pytest.raises(RpcTimeout):
            client.sleep(seconds=2)

        # make sure we can still use the client
        assert client.sleep(seconds=0) == 0


@pytest.mark.usefixtures('rabbit_config')
def test_no_timeout(container_factory, rabbit_manager):
    container = container_factory(FooService)
    container.start()

    with ServiceRpcClient('foobar') as client:
        with pytest.raises(eventlet.Timeout):
            with eventlet.Timeout(.1):
                client.sleep(seconds=1)


@pytest.mark.usefixtures('rabbit_config')
def test_async_timeout(container_factory, rabbit_manager):
    container = container_factory(FooService)
    container.start()

    with ServiceRpcClient('foobar', timeout=.5) as client:
        result = client.sleep.call_async(seconds=2)
        with pytest.raises(RpcTimeout):
            result.result()


@pytest.mark.usefixtures('rabbit_config')
def test_use_after_close(container_factory, rabbit_manager):
    container = container_factory(FooService)
    container.start()

    with ServiceRpcClient('foobar') as client:
        client.spam(ham=1)
        pass

    with pytest.raises(RuntimeError) as exc:
        client.spam(ham=1)
    assert 'can no longer be used' in str(exc)


@pytest.mark.usefixtures('rabbit_config')
@patch('nameko.standalone.rpc.RPC_REPLY_QUEUE_TTL', new=100)
def test_client_queue_expired_even_if_unused(rabbit_manager, get_vhost):
    vhost = get_vhost(config['AMQP_URI'])
    with ServiceRpcClient('exampleservice'):
        assert len(rabbit_manager.get_queues(vhost)) == 1

    eventlet.sleep(.15)  # sleep for >TTL
    assert len(rabbit_manager.get_queues(vhost)) == 0


@pytest.mark.usefixtures('rabbit_config')
def test_cluster_client(container_factory, rabbit_manager):
    container = container_factory(FooService)
    container.start()

    with ClusterRpcClient() as client:
        assert client.foobar.spam(ham=1) == 1


@pytest.mark.usefixtures('rabbit_config')
def test_cluster_client_reuse(container_factory, rabbit_manager):
    container = container_factory(FooService)
    container.start()

    cluster_client = ClusterRpcClient()
    with cluster_client as client:
        assert client.foobar.spam(ham=1) == 1

    with cluster_client as second_client:
        assert second_client.foobar.spam(ham=1) == 1


@pytest.mark.usefixtures('rabbit_config')
def test_cluster_client_dict_access(container_factory, rabbit_manager):
    container = container_factory(FooService)
    container.start()

    with ClusterRpcClient() as client:
        assert client['foobar'].spam(ham=3) == 3


@pytest.mark.usefixtures('rabbit_config')
def test_recover_from_keyboardinterrupt(container_factory, rabbit_manager):
    container = container_factory(FooService)
    container.start()  # create rpc queues
    container.stop()  # but make sure call doesn't complete

    with ServiceRpcClient('foobar') as client:

        with patch('kombu.connection.Connection.drain_events') as drain_events:
            drain_events.side_effect = KeyboardInterrupt('killing from test')
            with pytest.raises(KeyboardInterrupt):
                client.spam(ham=0)

        container = container_factory(FooService)
        container.start()

        # client should still work
        assert client.spam(ham=1) == 1


class TestConfigurability(object):
    """
    Test and demonstrate configuration options for the standalone RPC client
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
        """ Verify that most parameters can be specified at ServiceRpc
        instantiation time.
        """
        value = Mock()

        rpc_client = ClusterRpcClient(**{parameter: value})
        with rpc_client as client:
            client.service.method.call_async()
        assert producer.publish.call_args[1][parameter] == value

    @pytest.mark.usefixtures("memory_rabbit_config")
    @pytest.mark.usefixtures('predictable_call_ids')
    def test_headers(self, mock_container, producer):
        """ Headers can be provided at instantiation time, and are merged with
        Nameko headers.
        """
        data = {'context': 'data'}

        value = {'foo': Mock()}

        rpc_client = ClusterRpcClient(
            context_data=data, **{'headers': value}
        )

        with rpc_client as client:
            client['service-name'].method.call_async()

        def merge_dicts(base, *updates):
            merged = base.copy()
            [merged.update(update) for update in updates]
            return merged

        nameko_headers = {
            'nameko.context': 'data',
            'nameko.call_id_stack': ['standalone_rpc_client.0.0'],
        }

        assert producer.publish.call_args[1]['headers'] == merge_dicts(
            nameko_headers, value
        )

    @pytest.mark.usefixtures('rabbit_config')
    @patch('nameko.rpc.uuid')
    @patch('nameko.standalone.rpc.uuid')
    def test_restricted_parameters(
        self, patch_standalone_uuid, patch_uuid, mock_container, producer
    ):
        """ Verify that providing routing parameters at instantiation
        time has no effect.
        """
        patch_standalone_uuid.uuid4.return_value = "uuid1"
        patch_uuid.uuid4.return_value = "uuid2"

        restricted_params = (
            'exchange', 'routing_key', 'mandatory',
            'correlation_id', 'reply_to'
        )

        rpc_client = ClusterRpcClient(
            **{param: Mock() for param in restricted_params}
        )

        with rpc_client as client:
            client.service.method.call_async()

        publish_params = producer.publish.call_args[1]

        assert publish_params['exchange'].name == "nameko-rpc"
        assert publish_params['routing_key'] == 'service.method'
        assert publish_params['mandatory'] is True
        assert publish_params['reply_to'] == "uuid1"
        assert publish_params['correlation_id'] == "uuid2"


@pytest.mark.filterwarnings("ignore:Mandatory delivery:UserWarning")
@skip_if_no_toxiproxy
class TestStandaloneClientDisconnections(object):

    @pytest.fixture(autouse=True)
    def container(self, container_factory, rabbit_config):

        class Service(object):
            name = "service"

            @rpc
            def echo(self, arg):
                return arg

        container = container_factory(Service)
        container.start()

    @pytest.yield_fixture(autouse=True)
    def retry(self, request):
        retry = False
        if "publish_retry" in request.keywords:
            retry = True

        with patch.object(
            ServiceRpcClient.publisher_cls, 'retry', new=retry
        ):
            yield

    @pytest.yield_fixture(params=[True, False])
    def use_confirms(self, request):
        with patch.object(
            ServiceRpcClient.publisher_cls, 'use_confirms',
            new=request.param
        ):
            yield request.param

    @pytest.yield_fixture
    def service_rpc(self, rabbit_config, toxiproxy):
        with ServiceRpcClient("service", uri=toxiproxy.uri) as client:
            yield client

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
class TestStandaloneClientReplyListenerDisconnections(object):

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

    @pytest.fixture(autouse=True)
    def container(self, container_factory, rabbit_config):

        class Service(object):
            name = "service"

            @rpc
            def echo(self, arg):
                return arg

        # very fast heartbeat (2 seconds)
        with config.patch({HEARTBEAT_CONFIG_KEY: 2}):
            container = container_factory(Service)
            container.start()

    @pytest.fixture
    def rpc_client(self, rabbit_config, toxic_reply_listener):
        return ServiceRpcClient('service')

    @pytest.fixture
    def reply_listener(self, rpc_client):
        return rpc_client.reply_listener

    @pytest.yield_fixture
    def service_rpc(self, rpc_client):
        with rpc_client as client:
            yield client

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
            reply_listener.consumer, 'on_connection_error', callback=reset
        ):
            toxiproxy.disable()

            # connection re-established after one error;
            # make the request _inside_ the context block because the reply
            # listener doesn't establish a connection until consuming a result
            assert service_rpc.echo("foo") == "foo"

    def test_upstream_timeout(self, reply_listener, service_rpc, toxiproxy):
        """ Verify we detect and recover from sockets timing out.

        This failure mode means that the socket between the consumer and the
        rabbit broker times out after `timeout` milliseconds and then closes.

        Attempting to read from the socket after it's closed raises a
        socket.error and the connection will be re-established. If `timeout`
        is longer than twice the heartbeat interval, the behaviour is the same
        as in `test_upstream_blackhole` below.
        """
        def reset(args, kwargs, result, exc_info):
            toxiproxy.reset_timeout()
            return True

        with wait_for_call(
            reply_listener.consumer, 'on_connection_error', callback=reset
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
            reply_listener.consumer, 'on_connection_error', callback=reset
        ):
            toxiproxy.set_timeout(timeout=0)

            # connection re-established after one error;
            # make the request _inside_ the context block because the reply
            # listener doesn't establish a connection until consuming a result
            assert service_rpc.echo("foo") == "foo"

    def test_downstream_timeout(self, reply_listener, service_rpc, toxiproxy):
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
        def reset(args, kwargs, result, exc_info):
            toxiproxy.reset_timeout()
            return True

        with wait_for_call(
            reply_listener.consumer, 'on_connection_error', callback=reset
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
            reply_listener.consumer, 'on_connection_error', callback=reset
        ):
            toxiproxy.set_timeout(stream="downstream", timeout=0)

            # connection re-established after one error;
            # make the request _inside_ the context block because the reply
            # listener doesn't establish a connection until consuming a result
            assert service_rpc.echo("foo") == "foo"


class TestSSL(object):

    @pytest.mark.usefixtures("rabbit_config")
    def test_rpc_client_over_ssl(
        self, container_factory, rabbit_ssl_uri, rabbit_ssl_options
    ):
        class Service(object):
            name = "service"

            @rpc
            def echo(self, *args, **kwargs):
                return args, kwargs

        container = container_factory(Service)
        container.start()

        ssl = rabbit_ssl_options
        uri = rabbit_ssl_uri

        with ServiceRpcClient(
            "service",
            uri=uri,
            ssl=ssl,
            reply_listener_uri=config["AMQP_URI"],
            reply_listener_ssl=False,
        ) as client:
            assert client.echo("a", "b", foo="bar") == [
                ['a', 'b'], {'foo': 'bar'}
            ]
