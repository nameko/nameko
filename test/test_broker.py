import uuid

import eventlet
import pytest
from eventlet.event import Event
from mock import ANY, Mock, call, patch

from nameko.events import event_handler
from nameko.rpc import RpcProxy, rpc
from nameko.standalone.events import event_dispatcher
from nameko.standalone.rpc import ServiceRpcProxy
from nameko.testing.services import dummy, entrypoint_hook
from nameko.testing.utils import (
    assert_stops_raising, get_rabbit_connections, reset_rabbit_connections
)


disconnect_now = Event()
disconnected = Event()
method_called = Mock()
handle_called = Mock()

long_called = Event()


@pytest.yield_fixture(autouse=True)
def reset():
    yield
    method_called.reset_mock()
    handle_called.reset_mock()
    for event in (disconnect_now, disconnected):
        if event.ready():
            event.reset()


@pytest.yield_fixture
def logger():
    with patch('nameko.rpc._log', autospec=True) as patched:
        yield patched
    patched.reset_mock()


class ExampleService(object):
    name = "exampleservice"

    @rpc
    def echo(self, arg):
        return arg

    @rpc
    def method(self, arg):
        # trigger a disconnect and wait for confirmation
        # the message will be redelivered to the next consumer on
        # disconnection, so only try to disconnect the first time
        method_called(arg)
        if not disconnect_now.ready():
            disconnect_now.send(True)
            disconnected.wait()
            return arg
        return "duplicate-call-result"

    @event_handler('srcservice', 'exampleevent')
    def handle(self, evt_data):
        handle_called(evt_data)
        if not disconnect_now.ready():
            disconnect_now.send(True)
            disconnected.wait()


class ProxyService(object):
    name = "proxyservice"

    example_rpc = RpcProxy('exampleservice')

    @dummy
    def entrypoint(self, arg):
        return self.example_rpc.method(arg)


def disconnect_on_event(rabbit_manager, connection_name):
    disconnect_now.wait()
    rabbit_manager.delete_connection(connection_name)
    disconnected.send(True)


def test_idle_disconnect(container_factory, rabbit_manager, rabbit_config):
    """ Break the connection to rabbit while a service is started but idle
    (i.e. without active workers)
    """
    container = container_factory(ExampleService, rabbit_config)
    container.start()

    vhost = rabbit_config['vhost']
    reset_rabbit_connections(vhost, rabbit_manager)

    with ServiceRpcProxy('exampleservice', rabbit_config) as proxy:
        assert proxy.echo("hello") == "hello"


def test_proxy_disconnect_with_active_worker(
        container_factory, rabbit_manager, rabbit_config):
    """ Break the connection to rabbit while a service's queue consumer and
    rabbit while the service has an in-flight rpc request (i.e. it is waiting
    on a reply).
    """
    # ExampleService is the target; ProxyService has the rpc_proxy;
    proxy_container = container_factory(ProxyService, rabbit_config)
    example_container = container_factory(ExampleService, rabbit_config)

    proxy_container.start()

    # get proxyservice's queue consumer connection while we know it's the
    # only active connection
    vhost = rabbit_config['vhost']
    connections = get_rabbit_connections(vhost, rabbit_manager)
    assert len(connections) == 1
    proxy_consumer_conn = connections[0]['name']

    example_container.start()

    # there should now be two connections:
    # 1. the queue consumer from proxyservice
    # 2. the queue consumer from exampleservice
    connections = get_rabbit_connections(vhost, rabbit_manager)
    assert len(connections) == 2

    # disconnect proxyservice's queue consumer while its request is in-flight;
    # result will be returned on reconnection
    eventlet.spawn(disconnect_on_event, rabbit_manager, proxy_consumer_conn)
    with entrypoint_hook(proxy_container, 'entrypoint') as entrypoint:
        assert entrypoint('hello') == 'hello'

    # assert original connection no longer exists
    connections = get_rabbit_connections(vhost, rabbit_manager)
    assert proxy_consumer_conn not in [conn['name'] for conn in connections]


def test_service_disconnect_with_active_async_worker(
        container_factory, rabbit_manager, rabbit_config):
    """ Break the connection between a service's queue consumer and rabbit
    while the service has an active async worker (e.g. event handler).
    """
    container = container_factory(ExampleService, rabbit_config)
    container.start()

    # get the service's queue consumer connection while we know it's the
    # only active connection
    vhost = rabbit_config['vhost']
    connections = get_rabbit_connections(vhost, rabbit_manager)
    assert len(connections) == 1
    queue_consumer_conn = connections[0]['name']

    # disconnect the service's queue consumer while it's running the worker
    eventlet.spawn(disconnect_on_event, rabbit_manager, queue_consumer_conn)

    # dispatch an event
    data = uuid.uuid4().hex
    dispatch = event_dispatcher(rabbit_config)
    dispatch('srcservice', 'exampleevent', data)

    # `handle` will have been called twice with the same the `data`, because
    # rabbit will have redelivered the un-ack'd message from the first call
    def event_handled_twice():
        assert handle_called.call_args_list == [call(data), call(data)]
    assert_stops_raising(event_handled_twice)

    connections = get_rabbit_connections(vhost, rabbit_manager)
    assert queue_consumer_conn not in [conn['name'] for conn in connections]


def test_service_disconnect_with_active_rpc_worker(
        container_factory, rabbit_manager, rabbit_config):
    """ Break the connection between a service's queue consumer and rabbit
    while the service has an active rpc worker (i.e. response required).
    """
    container = container_factory(ExampleService, rabbit_config)
    container.start()

    # get the service's queue consumer connection while we know it's the
    # only active connection
    vhost = rabbit_config['vhost']
    connections = get_rabbit_connections(vhost, rabbit_manager)
    assert len(connections) == 1
    queue_consumer_conn = connections[0]['name']

    # create a standalone RPC proxy towards the target service
    rpc_proxy = ServiceRpcProxy('exampleservice', rabbit_config)
    proxy = rpc_proxy.start()

    # there should now be two connections:
    # 1. the queue consumer from the target service
    # 2. the queue consumer in the standalone rpc proxy
    connections = get_rabbit_connections(vhost, rabbit_manager)
    assert len(connections) == 2

    # disconnect the service's queue consumer while it's running a worker
    eventlet.spawn(disconnect_on_event, rabbit_manager, queue_consumer_conn)

    # we should receive the response from the first call
    # the standalone RPC proxy will stop listening as soon as it receives
    # a reply, so the duplicate response is discarded
    arg = uuid.uuid4().hex
    assert proxy.method(arg) == arg

    # `method` will have been called twice with the same the `arg`, because
    # rabbit will have redelivered the un-ack'd message from the first call
    def method_called_twice():
        assert method_called.call_args_list == [call(arg), call(arg)]
    assert_stops_raising(method_called_twice)

    connections = get_rabbit_connections(vhost, rabbit_manager)
    assert queue_consumer_conn not in [conn['name'] for conn in connections]

    rpc_proxy.stop()


def test_service_disconnect_with_active_rpc_worker_via_service_proxy(
        logger, container_factory, rabbit_manager, rabbit_config):
    """ Break the connection between a service's queue consumer and rabbit
    while the service has an active rpc worker (i.e. response required).

    Make the rpc call from a nameko service. We expect the service to see
    the duplicate response and discard it.
    """
    # ExampleService is the target; ProxyService has the rpc_proxy;
    proxy_container = container_factory(ProxyService, rabbit_config)
    service_container = container_factory(ExampleService, rabbit_config)

    service_container.start()

    # get exampleservice's queue consumer connection while we know it's the
    # only active connection
    vhost = rabbit_config['vhost']
    connections = get_rabbit_connections(vhost, rabbit_manager)
    assert len(connections) == 1
    service_consumer_conn = connections[0]['name']

    proxy_container.start()

    # there should now be two connections:
    # 1. the queue consumer from proxyservice
    # 2. the queue consumer from exampleservice
    connections = get_rabbit_connections(vhost, rabbit_manager)
    assert len(connections) == 2

    # disconnect exampleservice's queue consumer while it's running the worker
    eventlet.spawn(disconnect_on_event, rabbit_manager, service_consumer_conn)

    # we should receive the response from the first call
    # the service rpc_proxy will receive and discard the response from the
    # second call
    arg = uuid.uuid4().hex
    with entrypoint_hook(proxy_container, 'entrypoint') as entrypoint:
        # we should receive a response after reconnection
        assert entrypoint(arg) == arg

    def duplicate_response_received():
        correlation_warning = call("Unknown correlation id: %s", ANY)
        assert correlation_warning in logger.debug.call_args_list
    assert_stops_raising(duplicate_response_received)

    # `method` will have been called twice with the same the `arg`, because
    # rabbit will have redelivered the un-ack'd message from the first call
    def method_called_twice():
        assert method_called.call_args_list == [call(arg), call(arg)]
    assert_stops_raising(method_called_twice)

    connections = get_rabbit_connections(vhost, rabbit_manager)
    assert service_consumer_conn not in [conn['name'] for conn in connections]
