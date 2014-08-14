from collections import defaultdict
from functools import partial

from mock import Mock, patch, MagicMock, ANY
import pytest

from nameko.containers import ServiceContainer, WorkerContext
from nameko.exceptions import RemoteError, ContainerBeingKilled
from nameko.legacy.dependencies import rpc, NovaRpcProvider, NovaResponder
from nameko.legacy.proxy import RPCProxy
from nameko.messaging import AMQP_URI_CONFIG_KEY


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


@patch('nameko.legacy.dependencies.producers',
       new_callable=partial(defaultdict, MagicMock))
def test_nova_responder(producers):

    container = Mock()
    container.config = {AMQP_URI_CONFIG_KEY: ''}

    responder = NovaResponder("msgid")

    # serialisable result
    result, exc_info = responder.send_response(container, True, None)
    assert result is True
    assert exc_info is None

    mock_producer = producers.values()[0].acquire().__enter__()
    published_msgs = []
    for (args, _) in mock_producer.publish.call_args_list:
        published_msgs.append(args[0])

    assert published_msgs == [{
        'failure': None,
        'result': True,
        'ending': False
    }, {
        'failure': None,
        'result': None,
        'ending': True
    }]

    # unserialisable result
    producers.clear()
    obj = object()
    result, exc_info = responder.send_response(container, obj, None)
    assert result is None
    assert exc_info == (TypeError, ANY, ANY)

    mock_producer = producers.values()[0].acquire().__enter__()
    published_msgs = []
    for (args, _) in mock_producer.publish.call_args_list:
        published_msgs.append(args[0])

    assert published_msgs == [{
        'failure': ('TypeError', "{} is not JSON serializable".format(obj)),
        'result': None,
        'ending': False
    }, {
        'failure': None,
        'result': None,
        'ending': True
    }]

    # un-str-able exception

    class BadException(Exception):
        def __str__(self):
            raise Exception('boom')

    producers.clear()
    exc = BadException()
    result, exc_info = responder.send_response(
        container, True, (BadException, exc, "tb"))
    assert result is True
    assert exc_info == (BadException, exc, "tb")

    mock_producer = producers.values()[0].acquire().__enter__()
    published_msgs = []
    for (args, _) in mock_producer.publish.call_args_list:
        published_msgs.append(args[0])

    assert published_msgs == [{
        'failure': ('BadException', "[__str__ failed]"),
        'result': True,
        'ending': False
    }, {
        'failure': None,
        'result': None,
        'ending': True
    }]
