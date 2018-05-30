import itertools

import eventlet
import pytest
from eventlet.event import Event
from mock import Mock, call, patch
from requests import HTTPError, Response

from nameko.containers import ServiceContainer
from nameko.messaging import QueueConsumer
from nameko.rpc import Rpc, RpcConsumer, rpc
from nameko.testing.rabbit import Client
from nameko.testing.utils import (
    AnyInstanceOf, ResourcePipeline, find_free_port, get_container,
    get_extension, get_rabbit_connections, reset_rabbit_connections,
    wait_for_call
)


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
            pass  # pragma: no cover


def test_get_extension(rabbit_config):

    class Service(object):
        name = "service"

        @rpc
        def foo(self):
            pass  # pragma: no cover

        @rpc
        def bar(self):
            pass  # pragma: no cover

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


def test_rabbit_connection_refused_error():

    # port 4 is an official unassigned port, so no one should be using it
    bad_port_uri = 'http://localhost:4'
    with pytest.raises(Exception) as exc_info:
        Client(bad_port_uri)

    message = str(exc_info.value)
    assert 'Connection error' in message


@patch('nameko.testing.utils.socket')
class TestFindFreePort(object):

    def test_default_host(self, patched_socket):
        host = '127.0.0.1'
        free_port = 9999
        mock_sock = patched_socket.socket()
        mock_sock.getsockname.return_value = [host, free_port]

        assert find_free_port() == free_port
        assert mock_sock.bind.call_args_list == [call((host, 0))]
        assert mock_sock.close.called

    def test_specified_host(self, patched_socket):
        host = '10.0.0.1'
        free_port = 9999
        mock_sock = patched_socket.socket()
        mock_sock.getsockname.return_value = [host, free_port]

        assert find_free_port(host) == free_port
        assert mock_sock.bind.call_args_list == [call((host, 0))]
        assert mock_sock.close.called


class TestResourcePipeline(object):

    @pytest.mark.parametrize('size', [1, 5, 1000])
    def test_pipeline(self, size):
        created = []
        destroyed = []

        counter = itertools.count()

        def create():
            obj = next(counter)
            created.append(obj)
            return obj

        def destroy(obj):
            destroyed.append(obj)

        with ResourcePipeline(create, destroy, size).run() as pipeline:

            # check initial size
            eventlet.sleep()  # let pipeline fill up
            # when full, created is always exactly one more than `size`
            assert pipeline.ready.qsize() == size
            assert len(created) == size + 1

            # get an item
            with pipeline.get() as item:
                # let pipeline process
                eventlet.sleep()
                # expect pipeline to have created another item
                assert pipeline.ready.qsize() == size
                assert len(created) == size + 2

            # after putting the item back
            # let pipeline process
            eventlet.sleep()
            # expect item to have been destroyed
            assert pipeline.trash.qsize() == 0
            assert destroyed == [item]

        # after shutdown (no need to yield because shutdown is blocking)
        # expect all created items to have been destroyed
        assert created == destroyed == list(range(size + 2))

    def test_create_shutdown_race(self):
        """
        Test the race condition where the pipeline shuts down while
        `create` is still executing.
        """
        created = []
        destroyed = []

        counter = itertools.count()
        creating = Event()

        def create():
            creating.send(True)
            eventlet.sleep()
            obj = next(counter)
            created.append(obj)
            return obj

        def destroy(obj):
            destroyed.append(obj)

        with ResourcePipeline(create, destroy).run():
            creating.wait()
            assert created == []

        assert created == destroyed == list(range(1))

    def test_shutdown_immediately(self):

        created = []
        destroyed = []

        counter = itertools.count()

        def create():  # pragma: no cover
            obj = next(counter)
            created.append(obj)
            return obj

        def destroy(obj):  # pragma: no cover
            destroyed.append(obj)

        with ResourcePipeline(create, destroy).run():
            pass

        assert created == destroyed == []

    def test_zero_size(self):

        with pytest.raises(RuntimeError):
            ResourcePipeline(None, None, 0)
