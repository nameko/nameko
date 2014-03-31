from mock import ANY, Mock, patch
import pytest

from nameko.legacy import proxy
from nameko.legacy.testing import MockRPCProxy


def test_anon_context_constructor():
    context = proxy.get_anon_context()

    assert context.user_id is None


@pytest.mark.parametrize('constructor', [proxy.RPCProxy, MockRPCProxy])
@patch.object(proxy, 'nova', autospec=True)
def test_call(nova, constructor):
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

    nova.call.assert_called_once_with(
        connection, context, 'service',
        {'method': 'controller', 'args': {'key': 'value'}},
        options=rpcproxy.call_options(),
        timeout=None)


@pytest.mark.parametrize('constructor', [proxy.RPCProxy, MockRPCProxy])
@patch.object(proxy, 'nova', autospec=True)
def test_call_dynamic_route(nova, constructor):
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

    nova.call.assert_called_once_with(
        connection, context, 'service',
        {'method': 'controller', 'args': {'key': 'value'}},
        options=rpcproxy.call_options(),
        timeout=None)


@pytest.mark.parametrize('constructor', [proxy.RPCProxy, MockRPCProxy])
@patch.object(proxy, 'nova', autospec=True)
def test_call_default(nova, constructor):
    connection = ANY
    context = Mock()
    context_factory = lambda: context
    rpcproxy = constructor(context_factory=context_factory)

    rpcproxy.service.controller(key='value')

    nova.call.assert_called_once_with(
        connection, context, 'service',
        {'method': 'controller', 'args': {'key': 'value'}},
        options=rpcproxy.call_options(),
        timeout=None)


@pytest.mark.parametrize('constructor', [proxy.RPCProxy, MockRPCProxy])
@patch.object(proxy, 'nova', autospec=True)
def test_extra_route(nova, constructor):
    rpcproxy = constructor()

    with pytest.raises(AttributeError):
        rpcproxy.service.controller.extra(key='value')


@pytest.mark.parametrize('constructor', [proxy.RPCProxy, MockRPCProxy])
@patch.object(proxy, 'nova', autospec=True)
def test_route_abuse(nova, constructor):
    rpcproxy = constructor()
    # N.B. There are safeguards against misconfiguring the info attribute.
    #      If it's got to this point someone has been misusing the api.
    rpcproxy.info = ['service', 'controller', 'extra']

    with pytest.raises(ValueError):
        rpcproxy.call(key='value')


@pytest.mark.parametrize('constructor', [proxy.RPCProxy, MockRPCProxy])
@patch.object(proxy, 'nova', autospec=True)
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


# MockRPCProxy Tests

def test_add_dummy():
    rpcproxy = MockRPCProxy()
    rpcproxy.fallback_to_call = False

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
    rpcproxy.fallback_to_call = False

    assert rpcproxy._calls == []

    with pytest.raises(RuntimeError):
        rpcproxy.service.controller()


def test_add_call_matching():
    rpcproxy = MockRPCProxy()
    rpcproxy.fallback_to_call = False

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
    rpcproxy.fallback_to_call = False

    assert rpcproxy._calls == []

    with pytest.raises(RuntimeError):
        rpcproxy.service.controller(key='value')


@patch.object(proxy, 'nova', autospec=True)
def test_service_whitelist(nova):
    connection = ANY
    context = Mock()
    context_factory = lambda: context
    rpcproxy = MockRPCProxy(
        context_factory=context_factory)
    rpcproxy.fallback_to_call = False

    with pytest.raises(RuntimeError):
        rpcproxy.service.controller()

    assert nova.call.call_count == 0

    rpcproxy.add_service_to_whitelist('service')

    rpcproxy.service.controller()

    nova.call.assert_called_once_with(
        connection, context, 'service',
        {'method': 'controller', 'args': {}},
        options=rpcproxy.call_options(),
        timeout=None)

    with pytest.raises(RuntimeError):
        rpcproxy.blacklisted_service.controller()

    rpcproxy.reset()
    rpcproxy.fallback_to_call = False

    with pytest.raises(RuntimeError):
        rpcproxy.service.controller()

    assert nova.call.call_count == 1


def test_fallback_to_call():
    rpcproxy = MockRPCProxy()
    assert rpcproxy.fallback_to_call is True  # default
    rpcproxy.fallback_to_call = False
    assert rpcproxy.fallback_to_call is False


@pytest.mark.parametrize('constructor', [proxy.RPCProxy, MockRPCProxy])
@patch.object(proxy, 'nova', autospec=True)
def test_timeout(nova, constructor):
    connection = ANY
    context = Mock()
    context_factory = lambda: context

    # test null timeout
    rpcproxy = constructor(context_factory=context_factory)
    rpcproxy.service.controller(key='value')

    nova.call.assert_called_once_with(
        connection, context, 'service',
        {'method': 'controller', 'args': {'key': 'value'}},
        options=rpcproxy.call_options(),
        timeout=None)
    nova.reset_mock()

    # test default timeout
    rpcproxy = constructor(timeout=10, context_factory=context_factory)
    rpcproxy.service.controller(key='value')

    nova.call.assert_called_once_with(
        connection, context, 'service',
        {'method': 'controller', 'args': {'key': 'value'}},
        options=rpcproxy.call_options(),
        timeout=10)
    nova.reset_mock()

    # test override timeout
    rpcproxy = constructor(timeout=10, context_factory=context_factory)
    rpcproxy.service.controller(key='value', timeout=99)

    nova.call.assert_called_once_with(
        connection, context, 'service',
        {'method': 'controller', 'args': {'key': 'value'}},
        options=rpcproxy.call_options(),
        timeout=99)
