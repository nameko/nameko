from urlparse import urlparse, urlunparse

from amqp.exceptions import AccessRefused
import eventlet
from kombu.connection import Connection
import pytest

from nameko.events import Event, EventDispatcher, event_handler
from nameko.rpc import rpc
from nameko.standalone.rpc import ClusterRpcProxy
from nameko.testing.services import entrypoint_waiter, dummy, entrypoint_hook
from nameko.testing.utils import get_rabbit_config


# TEMP
class ExampleEvent(Event):
    type = "example"


class Service(object):

    dispatch = EventDispatcher()

    @rpc
    def echo(self, arg):
        return arg

    @rpc
    def send_event(self):
        self.dispatch(ExampleEvent("data"))


@pytest.yield_fixture(autouse=True)
def raise_channel_errors():
    # kombu.mixins.ConsumerMixin.run swallows all channel errors, including
    # amqp.exceptions.AccessDenied, and attempts to reconnect.
    channel_errors = Connection.channel_errors
    Connection.channel_errors = tuple()
    yield
    Connection.channel_errors = channel_errors


@pytest.yield_fixture
def restricted_config(request, rabbit_manager):
    username = "{}_user".format(request.function.__name__)
    rabbit_manager.create_user(username, username)

    amqp_uri = request.config.getoption('AMQP_URI')
    uri = urlparse(amqp_uri)
    netloc = "{}:{}@{}:{}".format(username, username, uri.hostname, uri.port)

    uri_parts = list(uri)
    uri_parts[1] = netloc
    restricted_uri = urlunparse(uri_parts)

    yield get_rabbit_config(restricted_uri)
    rabbit_manager.delete_user(username)


@pytest.yield_fixture(autouse=True)
def cluster(runner_factory, rabbit_config):

    class ServiceA(Service):
        name = "service_a"

    class ServiceB(Service):
        name = "service_b"

    runner = runner_factory(rabbit_config, ServiceA, ServiceB)
    runner.start()
    yield runner


def test_cluster_rpc(rabbit_manager, restricted_config, container_factory):

    vhost = restricted_config['vhost']
    username = restricted_config['username']

    # exchange.declare (conf@exchange); queue.declare (conf@queue)
    conf = "rpc\.reply-.*|nameko-rpc"
    # basic.consume (read@queue); queue.bind (read@exchange)
    read = "rpc\.reply-.*|nameko-rpc"
    # queue.bind (write@queue); basic.publish (write@exchange)
    write = "rpc\.reply-.*|nameko-rpc"

    rabbit_manager.set_vhost_permissions(vhost, username, conf, read, write)

    # can talk to existing services
    with ClusterRpcProxy(restricted_config) as proxy:
        assert proxy.service_a.echo("hello") == "hello"
        assert proxy.service_b.echo("hello") == "hello"

    # cannot steal from existing services
    class SpoofingService(object):
        name = "service-a"

        @rpc
        def echo(self, arg):
            return arg

    with pytest.raises(AccessRefused) as exc:
        container = container_factory(SpoofingService, restricted_config)
        container.start()
        with eventlet.Timeout():
            container.wait()
    # queue.declare requires conf on 'rpc-service-a'
    assert "access to queue 'rpc-service-a'" in exc.value.message

    # cannot start new services
    class ServiceC(object):
        name = "service-c"

        @rpc
        def echo(self, arg):
            return arg

    with pytest.raises(AccessRefused) as exc:
        container = container_factory(ServiceC, restricted_config)
        container.start()
        with eventlet.Timeout():
            container.wait()
    # queue.declare requires conf on 'rpc-service-c'
    assert "access to queue 'rpc-service-c'" in exc.value.message

    # TODO: these will work while the RpcProxy declares exchanges
    # cannot delete exchange
    # cannot delete service queues


def test_service_rpc():
    # can't do this without service-specific exchanges
    pass


def test_cluster_event_handler(rabbit_manager, rabbit_config,
                               restricted_config, container_factory):

    vhost = restricted_config['vhost']
    username = restricted_config['username']

    # exchange.declare (conf@exchange); queue.declare (conf@queue)
    conf = "evt-\w+-\w+--.*|\w+\.events"
    # basic.consume (read@queue); queue.bind (read@exchange)
    read = "evt-\w+-\w+--.*|\w+\.events"
    # queue.bind (write@queue)
    write = "evt-\w+-\w+--.*"

    rabbit_manager.set_vhost_permissions(vhost, username, conf, read, write)

    # can listen to events from ServiceA
    class ListenerA(object):
        name = "listener_a"

        @event_handler('service_a', 'example')
        def handle(self, data):
            pass

    container = container_factory(ListenerA, restricted_config)
    container.start()

    with entrypoint_waiter(container, 'handle'):
        with ClusterRpcProxy(rabbit_config) as proxy:
            proxy.service_a.send_event()

    # can listen to events from ServiceB
    class ListenerB(object):
        name = "listener_b"

        @event_handler('service_b', 'example')
        def handle(self, data):
            pass

    container = container_factory(ListenerB, restricted_config)
    container.start()

    with entrypoint_waiter(container, 'handle'):
        with ClusterRpcProxy(rabbit_config) as proxy:
            proxy.service_b.send_event()

    # can (sadly) spoof event queues
    class SpoofingListener(object):
        name = "listener_a"

        @event_handler('service_a', 'example')
        def handle(self, data):
            pass

    container = container_factory(SpoofingListener, restricted_config)
    container.start()

    queue = rabbit_manager.get_queue(
        vhost, 'evt-service_a-example--listener_a.handle')
    assert len(queue['consumer_details']) == 2

    # cannot dispatch events

    class SpoofingSender(object):
        name = 'service_a'

        dispatch = EventDispatcher()

        @event_handler('service_a', 'example')
        def handle(self, data):
            pass

        @dummy
        def send_event(self):
            self.dispatch(ExampleEvent("data"))

    container = container_factory(SpoofingSender, restricted_config)
    container.start()

    # can't write to exchange (doesn't error without publish confirms)
    with pytest.raises(eventlet.Timeout):
        with entrypoint_waiter(container, 'handle', timeout=.5):
            with entrypoint_hook(container, 'send_event') as hook:
                hook()

    # TODO: these will work while the EventDispatcher declares exchanges
    # cannot delete exchange
    # cannot delete service queues


def test_service_event_handler(rabbit_manager, rabbit_config,
                               restricted_config, container_factory):

    vhost = restricted_config['vhost']
    username = restricted_config['username']

    # exchange.declare (conf@exchange); queue.declare (conf@queue)
    conf = "evt-service_a-\w+--.*|service_a.events"
    # basic.consume (read@queue); queue.bind (read@exchange)
    read = "evt-service_a-\w+--.*|service_a.events"
    # queue.bind (write@queue)
    write = "evt-service_a-\w+--.*"

    rabbit_manager.set_vhost_permissions(vhost, username, conf, read, write)

    # can listen to events from ServiceA
    class ListenerA(object):
        name = "listener_a"

        @event_handler('service_a', 'example')
        def handle(self, data):
            pass

    container = container_factory(ListenerA, restricted_config)
    container.start()

    with entrypoint_waiter(container, 'handle'):
        with ClusterRpcProxy(rabbit_config) as proxy:
            proxy.service_a.send_event()

    # cannot listen to events from ServiceB
    class ListenerB(object):
        name = "listener_b"

        @event_handler('service_b', 'example')
        def handle(self, data):
            pass

    with pytest.raises(AccessRefused) as exc:
        container = container_factory(ListenerB, restricted_config)
        container.start()
        with eventlet.Timeout():
            container.wait()
    assert "access to exchange 'service_b.events'" in exc.value.message

    # can (sadly) spoof event queues
    class SpoofingListener(object):
        name = "listener_a"

        @event_handler('service_a', 'example')
        def handle(self, data):
            pass

    container = container_factory(SpoofingListener, restricted_config)
    container.start()

    queue = rabbit_manager.get_queue(
        vhost, 'evt-service_a-example--listener_a.handle')
    assert len(queue['consumer_details']) == 2

    # cannot dispatch events

    class SpoofingSender(object):
        name = 'service_a'

        dispatch = EventDispatcher()

        @event_handler('service_a', 'example')
        def handle(self, data):
            pass

        @dummy
        def send_event(self):
            self.dispatch(ExampleEvent("data"))

    container = container_factory(SpoofingSender, restricted_config)
    container.start()

    # can't write to exchange (doesn't error without publish confirms)
    with pytest.raises(eventlet.Timeout):
        with entrypoint_waiter(container, 'handle', timeout=.5):
            with entrypoint_hook(container, 'send_event') as hook:
                hook()

    # TODO: these will work while the EventDispatcher declares exchanges
    # cannot delete exchange
    # cannot delete service queues


def test_method_event_handler():
    pass  # crazy?


def test_event_dispatcher(rabbit_manager, rabbit_config,
                          restricted_config, container_factory):

    vhost = restricted_config['vhost']
    username = restricted_config['username']

    # exchange.declare (conf@exchange)
    conf = "dispatcher.events"
    #
    read = ""
    # basic.publish (write@exchange)
    write = "dispatcher.events"

    rabbit_manager.set_vhost_permissions(vhost, username, conf, read, write)

    # can dispatch events
    class Dispatcher(object):

        dispatch = EventDispatcher()

        @dummy
        def send_event(self):
            self.dispatch(ExampleEvent("data"))

    class Listener(object):

        @event_handler('dispatcher', 'example')
        def handle(self, data):
            pass

    listener_container = container_factory(Listener, rabbit_config)
    listener_container.start()

    dispatcher_container = container_factory(Dispatcher, restricted_config)
    dispatcher_container.start()

    with entrypoint_waiter(listener_container, 'handle'):
        with entrypoint_hook(dispatcher_container, 'send_event') as hook:
            hook()

    # cannot dispatch events as another service
    class SpoofingSender(object):
        name = "service_a"

        dispatch = EventDispatcher()

        @dummy
        def send_event(self):
            self.dispatch(ExampleEvent("data"))

    with pytest.raises(AccessRefused) as exc:
        container = container_factory(SpoofingSender, restricted_config)
        container.start()
        with eventlet.Timeout():
            container.wait()
    assert "access to exchange 'service_a.events'" in exc.value.message
