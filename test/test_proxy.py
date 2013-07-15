from mock import ANY, Mock, patch
import pytest

from nameko import proxy
from nameko.testing.proxy import MockRPCProxy


def test_anon_context_constructor():
    context = proxy.get_anon_context()

    assert context.user_id == None


@pytest.mark.parametrize('constructor', [proxy.RPCProxy, MockRPCProxy])
@patch.object(proxy, 'rpc')
def test_call(rpc, constructor):
    connection = ANY
    context = Mock()
    context_factory = lambda: context
    rpcproxy = constructor(context_factory=context_factory)

    with pytest.raises(ValueError):
        # no topic, no method
        rpcproxy.call(key='value')

    with pytest.raises(ValueError):
        # no method
        rpcproxy.service.call(key='value')

    rpcproxy.service.controller.call(key='value')

    rpc.call.assert_called_once_with(
        connection, context, 'service',
        {'method': 'controller', 'args': {'key': 'value'}},
        options=rpcproxy.call_options(),
        timeout=None)


@pytest.mark.parametrize('constructor', [proxy.RPCProxy, MockRPCProxy])
@patch.object(proxy, 'rpc')
def test_call_dynamic_route(rpc, constructor):
    connection = ANY
    context = Mock()
    context_factory = lambda: context
    rpcproxy = constructor(context_factory=context_factory)

    with pytest.raises(ValueError):
        # no topic, no method
        rpcproxy.call(key='value')

    with pytest.raises(ValueError):
        # no method
        rpcproxy.call(topic='service', key='value')

    with pytest.raises(ValueError):
        # no topic
        rpcproxy.call(method='controller', key='value')

    rpcproxy.call(topic='service', method='controller', key='value')

    rpc.call.assert_called_once_with(
        connection, context, 'service',
        {'method': 'controller', 'args': {'key': 'value'}},
        options=rpcproxy.call_options(),
        timeout=None)


@pytest.mark.parametrize('constructor', [proxy.RPCProxy, MockRPCProxy])
@patch.object(proxy, 'rpc')
def test_call_default(rpc, constructor):
    connection = ANY
    context = Mock()
    context_factory = lambda: context
    rpcproxy = constructor(context_factory=context_factory)

    rpcproxy.service.controller(key='value')

    rpc.call.assert_called_once_with(
        connection, context, 'service',
        {'method': 'controller', 'args': {'key': 'value'}},
        options=rpcproxy.call_options(),
        timeout=None)


@pytest.mark.parametrize('constructor', [proxy.RPCProxy, MockRPCProxy])
@patch.object(proxy, 'rpc')
def test_extra_route(rpc, constructor):
    rpcproxy = constructor()

    with pytest.raises(AttributeError):
        rpcproxy.service.controller.extra(key='value')


@pytest.mark.parametrize('constructor', [proxy.RPCProxy, MockRPCProxy])
@patch.object(proxy, 'rpc')
def test_route_abuse(rpc, constructor):
    rpcproxy = constructor()
    # N.B. There are safeguards against misconfiguring the info attribute.
    #      If it's got to this point someone has been misusing the api.
    rpcproxy.info = ['service', 'controller', 'extra']

    with pytest.raises(ValueError):
        rpcproxy.call(key='value')


@pytest.mark.parametrize('constructor', [proxy.RPCProxy, MockRPCProxy])
@patch.object(proxy, 'rpc')
def test_control_exchange_config(rpc, constructor):
    connection = ANY
    context = Mock()
    context_factory = lambda: context
    rpcproxy = constructor(
        control_exchange='rpc', context_factory=context_factory)

    rpcproxy.service.controller(key='value')

    rpc.call.assert_called_once_with(
        connection, context, 'service',
        {'method': 'controller', 'args': {'key': 'value'}},
        options={'CONTROL_EXCHANGE': 'rpc'},
        timeout=None)


@pytest.mark.parametrize('constructor', [proxy.RPCProxy, MockRPCProxy])
@patch.object(proxy, 'rpc')
def test_cast(rpc, constructor):
    connection = ANY
    context = Mock()
    context_factory = lambda: context
    rpcproxy = constructor(context_factory=context_factory)

    with pytest.raises(ValueError):
        # no topic, no method
        rpcproxy.cast(key='value')

    with pytest.raises(ValueError):
        # no method
        rpcproxy.service.cast(key='value')

    rpcproxy.service.controller.cast(key='value')

    rpc.cast.assert_called_once_with(
        connection, context, 'service',
        {'method': 'controller', 'args': {'key': 'value'}},
        options=rpcproxy.call_options())


@pytest.mark.parametrize('constructor', [proxy.RPCProxy, MockRPCProxy])
@patch.object(proxy, 'rpc')
def test_cast_dynamic_route(rpc, constructor):
    connection = ANY
    context = Mock()
    context_factory = lambda: context
    rpcproxy = constructor(context_factory=context_factory)

    with pytest.raises(ValueError):
        # no topic, no method
        rpcproxy.cast(key='value')

    with pytest.raises(ValueError):
        # no method
        rpcproxy.cast(topic='service', key='value')

    with pytest.raises(ValueError):
        # no topic
        rpcproxy.cast(method='controller', key='value')

    rpcproxy.cast(topic='service', method='controller', key='value')

    rpc.cast.assert_called_once_with(
        connection, context, 'service',
        {'method': 'controller', 'args': {'key': 'value'}},
        options=rpcproxy.call_options())
