import eventlet
from eventlet.event import Event
from mock import Mock, patch, call
import pytest
from requests import Response, HTTPError

from nameko.constants import DEFAULT_MAX_WORKERS
from nameko.rpc import rpc, Rpc
from nameko.testing.rabbit import Client
from nameko.testing.utils import (
    AnyInstanceOf, get_extension, get_container, wait_for_call,
    reset_rabbit_vhost, get_rabbit_connections, wait_for_worker_idle,
    reset_rabbit_connections)


def test_any_instance_of():

    assert "" == AnyInstanceOf(str)
    assert 99 != AnyInstanceOf(str)

    class Foo(object):
        pass
    foo = Foo()

    assert foo == AnyInstanceOf(Foo)
    assert foo == AnyInstanceOf(object)
    assert foo != AnyInstanceOf(foo)

    assert repr(AnyInstanceOf(str)) == "<AnyInstanceOf-str>"
    assert repr(AnyInstanceOf(Foo)) == "<AnyInstanceOf-Foo>"

    assert AnyInstanceOf == AnyInstanceOf(type)
    assert str == AnyInstanceOf(type)
    assert int == AnyInstanceOf(type)
    assert type == AnyInstanceOf(type)


def test_wait_for_call():
    mock = Mock()

    def call_after(seconds):
        eventlet.sleep(seconds)
        mock.method()

    # should not raise
    eventlet.spawn(call_after, 0)
    with wait_for_call(1, mock.method):
        pass

    mock.reset_mock()

    with pytest.raises(eventlet.Timeout):
        eventlet.spawn(call_after, 1)
        with wait_for_call(0, mock.method):
            pass


def test_get_extension(rabbit_config):

    from nameko.messaging import QueueConsumer
    from nameko.rpc import Rpc, RpcConsumer
    from nameko.containers import ServiceContainer

    class Service(object):
        name = "service"

        @rpc
        def foo(self):
            pass

        @rpc
        def bar(self):
            pass

    container = ServiceContainer(Service, rabbit_config)

    rpc_consumer = get_extension(container, RpcConsumer)
    queue_consumer = get_extension(container, QueueConsumer)
    foo_rpc = get_extension(container, Rpc, method_name="foo")
    bar_rpc = get_extension(container, Rpc, method_name="bar")

    extensions = container.extensions
    assert extensions == set([rpc_consumer, queue_consumer, foo_rpc, bar_rpc])


def test_get_container(runner_factory, rabbit_config):

    class ServiceX(object):
        name = "service_x"

    class ServiceY(object):
        name = "service_y"

    runner = runner_factory(rabbit_config, ServiceX, ServiceY)

    assert get_container(runner, ServiceX).service_cls is ServiceX
    assert get_container(runner, ServiceY).service_cls is ServiceY
    assert get_container(runner, object) is None


def test_reset_rabbit_vhost(rabbit_config, rabbit_manager):

    vhost = rabbit_config['vhost']
    username = rabbit_config['username']

    def get_active_vhosts():
        return [vhost_data['name'] for
                vhost_data in rabbit_manager.get_all_vhosts()]

    reset_rabbit_vhost(vhost, username, rabbit_manager)
    assert vhost in get_active_vhosts()

    rabbit_manager.delete_vhost(vhost)
    assert vhost not in get_active_vhosts()

    reset_rabbit_vhost(vhost, username, rabbit_manager)
    assert vhost in get_active_vhosts()


def test_reset_rabbit_vhost_errors():

    rabbit_manager = Mock()

    # 500 error
    response_500 = Response()
    response_500.status_code = 50
    error_500 = HTTPError(response=response_500)
    rabbit_manager.delete_vhost.side_effect = error_500

    with pytest.raises(HTTPError):
        reset_rabbit_vhost("vhost", "username", rabbit_manager)

    # 404 error
    response_404 = Response()
    response_404.status_code = 404
    error_404 = HTTPError(response=response_404)
    rabbit_manager.delete_vhost.side_effect = error_404

    # does not raise
    reset_rabbit_vhost("vhost", "username", rabbit_manager)
    assert rabbit_manager.create_vhost.call_args == call("vhost")
    assert rabbit_manager.set_vhost_permissions.call_args == call(
        "vhost", "username", '.*', '.*', '.*')


def test_get_rabbit_connections():

    vhost = "vhost_name"

    connections = [{
        'vhost': vhost,
        'key': 'value'
    }, {
        'vhost': 'unlikely_vhost_name',
        'key': 'value'
    }]

    rabbit_manager = Mock()

    rabbit_manager.get_connections.return_value = connections
    vhost_conns = [connections[0]]
    assert get_rabbit_connections(vhost, rabbit_manager) == vhost_conns

    rabbit_manager.get_connections.return_value = None
    assert get_rabbit_connections(vhost, rabbit_manager) == []


def test_reset_rabbit_connections():

    with patch('nameko.testing.utils.get_rabbit_connections') as connections:
        connections.return_value = [{
            'vhost': 'vhost',
            'name': 'connection_name'
        }]

        rabbit_manager = Mock()
        reset_rabbit_connections('vhost', rabbit_manager)

        assert rabbit_manager.delete_connection.call_args_list == [
            call("connection_name")]


def test_reset_rabbit_connection_errors():

    rabbit_manager = Mock()

    with patch('nameko.testing.utils.get_rabbit_connections') as connections:
        connections.return_value = [{
            'vhost': 'vhost_name',
            'name': 'connection_name'
        }]

        # 500 error
        response_500 = Response()
        response_500.status_code = 500
        error_500 = HTTPError(response=response_500)
        rabbit_manager.delete_connection.side_effect = error_500

        with pytest.raises(HTTPError):
            reset_rabbit_connections("vhost_name", rabbit_manager)

        # 404 error
        response_404 = Response()
        response_404.status_code = 404
        error_404 = HTTPError(response=response_404)
        rabbit_manager.delete_connection.side_effect = error_404

        # does not raise
        reset_rabbit_connections("vhost_name", rabbit_manager)


def test_wait_for_worker_idle(container_factory, rabbit_config):

    event = Event()

    class Service(object):
        name = "service"

        @rpc
        def wait_for_event(self):
            event.wait()

    container = container_factory(Service, rabbit_config)
    container.start()

    max_workers = DEFAULT_MAX_WORKERS

    # verify nothing running
    assert container._worker_pool.free() == max_workers
    with eventlet.Timeout(1):
        wait_for_worker_idle(container)

    # spawn a worker
    wait_for_event = get_extension(container, Rpc)
    container.spawn_worker(wait_for_event, [], {})

    # verify that wait_for_worker_idle does not return while worker active
    assert container._worker_pool.free() == max_workers - 1
    gt = eventlet.spawn(wait_for_worker_idle, container)
    assert not gt.dead  # still waiting

    # verify that wait_for_worker_idle raises when it times out
    with pytest.raises(eventlet.Timeout):
        wait_for_worker_idle(container, timeout=0)

    # complete the worker, verify previous wait_for_worker_idle completes
    event.send()
    with eventlet.Timeout(1):
        gt.wait()
    assert container._worker_pool.free() == max_workers


def test_rabbit_connection_refused_error():

    # port 4 is an official unassigned port, so no one should be using it
    bad_port_uri = 'http://localhost:4'
    with pytest.raises(Exception) as exc_info:
        Client(bad_port_uri)

    message = str(exc_info.value)
    assert 'Connection error' in message
