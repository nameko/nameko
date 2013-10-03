from itertools import count
import pytest
import socket

import eventlet
from kombu import Connection
from mock import patch

from nameko.events import event_handler
from nameko.exceptions import RemoteError
from nameko.rpc import rpc
from nameko.messaging import AMQP_URI_CONFIG_KEY


class ExampleError(Exception):
    pass


class ExampleService(object):
    name = 'exampleservice'

    @rpc
    def task_a(self, *args, **kwargs):
        print "task_a", args, kwargs
        return "result_a"

    @rpc
    def task_b(self, *args, **kwargs):
        print "task_b", args, kwargs
        return "result_b"

    @rpc
    def broken(self):
        raise ExampleError("broken")

    @rpc
    def echo(self, *args, **kwargs):
        return args, kwargs

    @event_handler('srcservice', 'eventtype')
    def async_task(self):
        pass


class FailingConnection(Connection):
    exc_type = socket.error

    def __init__(self, *args, **kwargs):
        self.failure_count = 0
        self.max_failure_count = kwargs.pop('max_failure_count', count(0))
        return super(FailingConnection, self).__init__(*args, **kwargs)

    @property
    def should_raise(self):
        return self.failure_count < self.max_failure_count

    def maybe_raise(self, fun, *args, **kwargs):
        if self.should_raise:
            raise self.exc_type()
        return fun(*args, **kwargs)

    def ensure(self, obj, fun, **kwargs):
        def wrapped(*args, **kwargs):
            return self.maybe_raise(fun, *args, **kwargs)
        return super(FailingConnection, self).ensure(obj, wrapped, **kwargs)

    def connect(self, *args, **kwargs):
        if self.should_raise:
            self.failure_count += 1
            raise self.exc_type()
        return super(FailingConnection, self).connect(*args, **kwargs)


# test rpc proxy ...


def test_rpc_consumer_creates_single_consumer(container_factory, rabbit_config,
                                              rabbit_manager):
    container = container_factory(ExampleService, rabbit_config)
    container.start()

    # we should have a queue for RPC and a queue for events
    vhost = rabbit_config['vhost']
    queues = rabbit_manager.get_queues(vhost)
    assert len(queues) == 2

    # each one should have one consumer
    rpc_queue = rabbit_manager.get_queue(vhost, "rpc-exampleservice")
    assert len(rpc_queue['consumer_details']) == 1
    evt_queue = rabbit_manager.get_queue(
        vhost, "evt-srcservice-eventtype--exampleservice.async_task")
    assert len(evt_queue['consumer_details']) == 1

    # and both share a single connection
    connections = rabbit_manager.get_connections()
    assert len(connections) == 1


def test_rpc_args_kwargs(container_factory, rabbit_config,
                         service_proxy_factory):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    proxy = service_proxy_factory(container, "exampleservice")

    assert proxy.echo() == [[], {}]
    assert proxy.echo("a", "b") == [["a", "b"], {}]
    assert proxy.echo(foo="bar") == [[], {'foo': 'bar'}]
    assert proxy.echo("arg", kwarg="kwarg") == [["arg"], {'kwarg': 'kwarg'}]


def test_rpc_existing_method(container_factory, rabbit_config, rabbit_manager,
                             service_proxy_factory):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    proxy = service_proxy_factory(container, "exampleservice")

    assert proxy.task_a() == "result_a"
    assert proxy.task_b() == "result_b"


def test_rpc_missing_method(container_factory, rabbit_config, rabbit_manager,
                            service_proxy_factory):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    proxy = service_proxy_factory(container, "exampleservice")

    with pytest.raises(RemoteError) as exc_info:
        proxy.task_c()
    assert exc_info.value.exc_type == "MethodNotFound"


def test_rpc_broken_method(container_factory, rabbit_config,
                           rabbit_manager, service_proxy_factory):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    proxy = service_proxy_factory(container, "exampleservice")

    with pytest.raises(RemoteError) as exc_info:
        proxy.broken()
    assert exc_info.value.exc_type == "ExampleError"


def test_rpc_responder_auto_retries(container_factory, rabbit_config,
                                    rabbit_manager, service_proxy_factory):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    proxy = service_proxy_factory(container, "exampleservice")
    uri = container.ctx.config[AMQP_URI_CONFIG_KEY]
    conn = FailingConnection(uri, max_failure_count=2)

    with patch("kombu.connection.ConnectionPool.new") as new_connection:
        new_connection.return_value = conn

        assert proxy.task_a() == "result_a"
        assert conn.failure_count == 2


def test_rpc_responder_eventual_failure(container_factory, rabbit_config,
                                        rabbit_manager, service_proxy_factory):

    container = container_factory(ExampleService, rabbit_config)
    container.start()

    proxy = service_proxy_factory(container, "exampleservice")
    uri = container.ctx.config[AMQP_URI_CONFIG_KEY]
    conn = FailingConnection(uri)

    with patch("kombu.connection.ConnectionPool.new") as new_connection:
        new_connection.return_value = conn

        # how do i test that the service has really killed itself?

        # with eventlet.Timeout(15):
        # with pytest.raises(Exception):
        proxy.task_a()

    while True:
        eventlet.sleep()

# test reply-to and correlation-id correct
