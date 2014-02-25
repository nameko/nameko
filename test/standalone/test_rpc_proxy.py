from mock import patch
import pytest
import socket

from nameko.containers import WorkerContext
from nameko.dependencies import injection, InjectionProvider, DependencyFactory
from nameko.exceptions import RemoteError
from nameko.rpc import rpc
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

    path = 'nameko.standalone.rpc.PollingQueueConsumer.poll_messages'
    with patch(path, autospec=True) as poll_messages:
        poll_messages.side_effect = socket.error

        with RpcProxy("foobar", rabbit_config) as proxy:
            with pytest.raises(socket.error):
                proxy.spam("")
