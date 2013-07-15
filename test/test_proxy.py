from mock import ANY, Mock, patch
import pytest

from nameko import proxy
from nameko.testing.proxy import MockRPCProxy


def test_anon_context_constructor():
    context = proxy.get_anon_context()

    assert context.user_id is None


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


# MockRPCProxy Tests

def test_add_dummy():
    rpcproxy = MockRPCProxy(fallback_to_call=False)

    with pytest.raises(RuntimeError):
        rpcproxy.service.controller()

    response = Mock()
    rpcproxy.add_dummy('service', 'controller', response)

    assert rpcproxy._calls == [
        ('service', 'controller', {}),
    ]

    assert rpcproxy.service.controller() == response
    assert rpcproxy.service.controller(key='value') == response

    assert rpcproxy._calls == [
        ('service', 'controller', {}),
        ('service', 'controller', {}),
        ('service', 'controller', {'key': 'value'}),
    ]

    with pytest.raises(RuntimeError):
        rpcproxy.service.other_controller()

    assert rpcproxy._calls == [
        ('service', 'controller', {}),
        ('service', 'controller', {}),
        ('service', 'controller', {'key': 'value'}),
        ('service', 'other_controller', {}),
    ]

    rpcproxy.reset()

    assert rpcproxy._calls == []

    with pytest.raises(RuntimeError):
        rpcproxy.service.controller()


def test_add_call_matching():
    rpcproxy = MockRPCProxy(fallback_to_call=False)

    with pytest.raises(RuntimeError):
        rpcproxy.service.controller()

    response = Mock()
    rpcproxy.add_call_matching(
        'service', 'controller', {'key': 'value'}, response)

    assert rpcproxy._calls == [
        ('service', 'controller', {}),
    ]

    with pytest.raises(RuntimeError):
        rpcproxy.service.controller()

    assert rpcproxy.service.controller(key='value') == response

    assert rpcproxy._calls == [
        ('service', 'controller', {}),
        ('service', 'controller', {}),
        ('service', 'controller', {'key': 'value'}),
    ]

    with pytest.raises(RuntimeError):
        rpcproxy.service.other_controller()

    assert rpcproxy._calls == [
        ('service', 'controller', {}),
        ('service', 'controller', {}),
        ('service', 'controller', {'key': 'value'}),
        ('service', 'other_controller', {}),
    ]

    rpcproxy.reset()

    assert rpcproxy._calls == []

    with pytest.raises(RuntimeError):
        rpcproxy.service.controller(key='value')


@patch.object(proxy, 'rpc')
def test_service_whitelist(rpc):
    connection = ANY
    context = Mock()
    context_factory = lambda: context
    rpcproxy = MockRPCProxy(
        context_factory=context_factory, fallback_to_call=False)

    with pytest.raises(RuntimeError):
        rpcproxy.service.controller()

    assert rpc.call.call_count == 0

    rpcproxy.add_service_to_whitelist('service')

    rpcproxy.service.controller()

    rpc.call.assert_called_once_with(
        connection, context, 'service',
        {'method': 'controller', 'args': {}},
        options=rpcproxy.call_options(),
        timeout=None)

    with pytest.raises(RuntimeError):
        rpcproxy.blacklisted_service.controller()

    rpcproxy.reset()

    with pytest.raises(RuntimeError):
        rpcproxy.service.controller()

    rpc.call.call_count == 1
