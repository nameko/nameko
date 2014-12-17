import eventlet
from kombu.message import Message
from mock import patch
import pytest
import socket

from nameko.containers import WorkerContext
from nameko.dependencies import injection, InjectionProvider, DependencyFactory
from nameko.exceptions import RemoteError, RpcTimeout
from nameko.rpc import rpc, Responder
from nameko.standalone.rpc import RpcProxy
from nameko.testing.utils import get_rabbit_connections
from nameko.exceptions import RpcConnectionError


class ContextReader(InjectionProvider):
    """ Access values from the worker context data.

    This is a test facilty! Write specific InjectionProviders to make use of
    values in ``WorkerContext.data``, don't expose it directly.
    """
    def acquire_injection(self, worker_ctx):
        def get_context_value(key):
            return worker_ctx.data.get(key)
        return get_context_value


@injection
def context_reader():
    return DependencyFactory(ContextReader)


class FooService(object):
    name = 'foobar'

    get_context_value = context_reader()

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

    with RpcProxy('foobar', rabbit_config) as foo:
        assert foo.spam(ham='eggs') == 'eggs'
        assert foo.spam(ham='eggs') == 'eggs'  # test re-use


def test_proxy_manual_start_stop(container_factory, rabbit_config):

    container = container_factory(FooService, rabbit_config)
    container.start()

    foobar_proxy = RpcProxy('foobar', rabbit_config)
    foo = foobar_proxy.start()
    assert foo.spam(ham='eggs') == 'eggs'
    assert foo.spam(ham='eggs') == 'eggs'  # test re-use
    foobar_proxy.stop()


def test_proxy_context_data(container_factory, rabbit_config):

    container = container_factory(FooService, rabbit_config)
    container.start()

    context_data = {'language': 'en'}
    with RpcProxy('foobar', rabbit_config, context_data) as foo:
        assert foo.get_context_data('language') == 'en'

    context_data = {'language': 'fr'}
    with RpcProxy('foobar', rabbit_config, context_data) as foo:
        assert foo.get_context_data('language') == 'fr'


def test_proxy_worker_context(container_factory, rabbit_config):

    container = container_factory(FooService, rabbit_config,
                                  CustomWorkerContext)
    container.start()

    context_data = {'custom_header': 'custom_value'}

    with RpcProxy('foobar', rabbit_config, context_data,
                  worker_ctx_cls=CustomWorkerContext) as foo:
        assert foo.get_context_data('custom_header') == "custom_value"

    with RpcProxy('foobar', rabbit_config, context_data) as foo:
        assert foo.get_context_data('custom_header') is None


def test_proxy_remote_error(container_factory, rabbit_config):

    container = container_factory(FooService, rabbit_config)
    container.start()

    with RpcProxy("foobar", rabbit_config) as proxy:
        with pytest.raises(RemoteError) as exc_info:
            proxy.broken()
        assert exc_info.value.exc_type == "ExampleError"


def test_proxy_connection_error(container_factory, rabbit_config):

    container = container_factory(FooService, rabbit_config)
    container.start()

    with RpcProxy("foobar", rabbit_config) as proxy:
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

    with RpcProxy('foobar', rabbit_config) as foo:
        assert foo.spam(ham='eggs') == 'eggs'

    queues_after = list_queues()
    assert queues_after == queues_before

    # check proxy re-use
    with RpcProxy('foobar', rabbit_config) as foo:
        assert foo.spam(ham='eggs') == 'eggs'
        assert foo.spam(ham='eggs') == 'eggs'

    queues_after = list_queues()
    assert queues_after == queues_before


def test_unexpected_correlation_id(container_factory, rabbit_config):
    container = container_factory(FooService, rabbit_config)
    container.start()

    with RpcProxy("foobar", rabbit_config) as proxy:

        message = Message(channel=None, properties={
            'reply_to': proxy.reply_listener.routing_key,
            'correlation_id': 'invalid',
        })
        responder = Responder(message)
        with patch('nameko.standalone.rpc._logger', autospec=True) as logger:
            responder.send_response(container, None, None)
            assert proxy.spam(ham='eggs') == 'eggs'
            assert logger.debug.call_count == 1


def test_async_rpc(container_factory, rabbit_config):

    container = container_factory(FooService, rabbit_config)
    container.start()

    with RpcProxy('foobar', rabbit_config) as foo:
        rep1 = foo.spam.async(ham=1)
        rep2 = foo.spam.async(ham=2)
        rep3 = foo.spam.async(ham=3)
        rep4 = foo.spam.async(ham=4)
        rep5 = foo.spam.async(ham=5)
        assert rep2.result() == 2
        assert rep3.result() == 3
        assert rep1.result() == 1
        assert rep4.result() == 4
        assert rep5.result() == 5


def test_multiple_proxies(container_factory, rabbit_config):
    container = container_factory(FooService, rabbit_config)
    container.start()

    with RpcProxy('foobar', rabbit_config) as proxy1:
        res1 = proxy1.spam.async(ham=1)

        with RpcProxy('foobar', rabbit_config) as proxy2:
            res2 = proxy2.spam.async(ham=2)

            assert res1.result() == 1
            assert res2.result() == 2


def test_multiple_calls_to_result(container_factory, rabbit_config):
    container = container_factory(FooService, rabbit_config)
    container.start()

    with RpcProxy('foobar', rabbit_config) as proxy:
        res = proxy.spam.async(ham=1)
        res.result()
        res.result()


class ExampleService(object):
    def callback(self):
        # to be patched out with mock
        pass

    @rpc
    def method(self, arg):
        self.callback()
        return arg


def test_disconnect_with_pending_reply(
        container_factory, rabbit_manager, rabbit_config):

    example_container = container_factory(ExampleService, rabbit_config)
    example_container.start()

    vhost = rabbit_config['vhost']

    # get exampleservice's queue consumer connection while we know it's the
    # only active connection
    connections = get_rabbit_connections(vhost, rabbit_manager)
    assert len(connections) == 1
    container_connection = connections[0]

    with RpcProxy('exampleservice', rabbit_config) as proxy:
        connections = get_rabbit_connections(vhost, rabbit_manager)
        assert len(connections) == 2
        proxy_connection = [
            conn for conn in connections if conn != container_connection][0]

        def disconnect_once(self):
            if hasattr(disconnect_once, 'called'):
                return
            disconnect_once.called = True
            rabbit_manager.delete_connection(proxy_connection['name'])

        with patch.object(ExampleService, 'callback', disconnect_once):

            async = proxy.method.async('hello')

            # if disconnecting while waiting for a reply, call fails
            with pytest.raises(RpcConnectionError):
                proxy.method('hello')

            # the failure above also has to consider any other pending calls a
            # failure, since the reply may have been sent while the queue was
            # gone (deleted on disconnect, and not added until re-connect)
            with pytest.raises(RpcConnectionError):
                async.result()

            # proxy should work again afterwards
            assert proxy.method('hello') == 'hello'


def test_timeout_not_needed(container_factory, rabbit_manager, rabbit_config):
    container = container_factory(FooService, rabbit_config)
    container.start()

    with RpcProxy('foobar', rabbit_config, timeout=1) as proxy:
        assert proxy.sleep() == 0


def test_timeout(container_factory, rabbit_manager, rabbit_config):
    container = container_factory(FooService, rabbit_config)
    container.start()

    with RpcProxy('foobar', rabbit_config, timeout=.1) as proxy:
        with pytest.raises(RpcTimeout):
            proxy.sleep(seconds=1)

        # make sure we can still use the proxy
        assert proxy.sleep(seconds=0) == 0


def test_no_timeout(
    container_factory, rabbit_manager, rabbit_config
):
    container = container_factory(FooService, rabbit_config)
    container.start()

    with RpcProxy('foobar', rabbit_config) as proxy:
        with pytest.raises(eventlet.Timeout):
            with eventlet.Timeout(.1):
                proxy.sleep(seconds=1)


def test_async_timeout(
    container_factory, rabbit_manager, rabbit_config
):
    container = container_factory(FooService, rabbit_config)
    container.start()

    with RpcProxy('foobar', rabbit_config, timeout=.1) as proxy:
        result = proxy.sleep.async(seconds=1)
        with pytest.raises(RpcTimeout):
            result.result()

        result = proxy.sleep.async(seconds=.2)
        eventlet.sleep(.2)
        result.result()


def test_proxy_deletes_queue_even_if_unused(rabbit_manager, rabbit_config):
    vhost = rabbit_config['vhost']
    with RpcProxy('exampleservice', rabbit_config):
        assert len(rabbit_manager.get_queues(vhost)) == 1

    assert len(rabbit_manager.get_queues(vhost)) == 0
