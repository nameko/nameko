from mock import Mock
import pytest

from nameko.containers import ServiceContainer, WorkerContext
from nameko.exceptions import RemoteError, ContainerBeingKilled
from nameko.legacy.dependencies import rpc, NovaRpcProvider
from nameko.legacy.proxy import RPCProxy


class NovaService(object):

    @rpc
    def echo(self, arg):
        return arg


def test_nova_rpc(container_factory, rabbit_config):

    container = container_factory(NovaService, rabbit_config)
    container.start()

    uri = rabbit_config['AMQP_URI']
    proxy = RPCProxy(uri)

    assert proxy.novaservice.echo(arg="hello") == "hello"

    with pytest.raises(RemoteError) as exc:
        proxy.novaservice.not_a_method()
    assert "MethodNotFound" in exc.value.message

    with pytest.raises(RemoteError) as exc:
        proxy.novaservice.echo()
    assert "IncorrectSignature" in exc.value.message

    container.stop()


def test_nova_rpc_provider(empty_config):

    rpc_consumer = Mock()
    message = Mock(headers={})

    message_body = {
        'method': 'method',
        'args': {"arg": "arg_value"},
        'msg_id': 'msg_id',
        '_context_user_id': 'user_id'
    }

    class Service(object):
        def method(self, arg):
            pass

    container = Mock(spec=ServiceContainer)
    container.service_cls = Service
    container.worker_ctx_cls = WorkerContext
    container.service_name = "service"
    container.config = empty_config

    rpc_provider = NovaRpcProvider()
    rpc_provider.rpc_consumer = rpc_consumer
    rpc_provider.bind("method", container)

    container.spawn_worker.side_effect = ContainerBeingKilled()
    rpc_provider.handle_message(message_body, message)
    assert rpc_consumer.requeue_message.called
