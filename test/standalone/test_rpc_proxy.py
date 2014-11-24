from kombu.message import Message
from mock import patch
import pytest
import socket

from nameko.containers import WorkerContext
from nameko.dependencies import injection, InjectionProvider, DependencyFactory
from nameko.exceptions import RemoteError
from nameko.rpc import rpc, Responder
from nameko.standalone.rpc import RpcProxy


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
                  CustomWorkerContext) as foo:
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
