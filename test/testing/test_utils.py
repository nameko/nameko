import eventlet
from mock import Mock
import pytest

from nameko.testing.utils import (
    AnyInstanceOf, get_dependency, get_container, wait_for_call,
    reset_rabbit_vhost)


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


def test_get_dependency(rabbit_config):

    from nameko.messaging import QueueConsumer
    from nameko.rpc import rpc, RpcProvider, RpcConsumer
    from nameko.containers import ServiceContainer, WorkerContext

    class Service(object):
        @rpc
        def foo(self):
            pass

        @rpc
        def bar(self):
            pass

    container = ServiceContainer(Service, WorkerContext, rabbit_config)

    rpc_consumer = get_dependency(container, RpcConsumer)
    queue_consumer = get_dependency(container, QueueConsumer)
    foo_rpc = get_dependency(container, RpcProvider, name="foo")
    bar_rpc = get_dependency(container, RpcProvider, name="bar")

    all_deps = container.dependencies
    assert all_deps == set([rpc_consumer, queue_consumer, foo_rpc, bar_rpc])


def test_get_container(runner_factory, rabbit_config):

    class ServiceX(object):
        name = "service_x"

    class ServiceY(object):
        name = "service_y"

    runner = runner_factory(rabbit_config, ServiceX, ServiceY)
    runner.start()

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
