import uuid

from mock import Mock, call
import pytest

from nameko.events import event_handler, Event
from nameko.exceptions import RemoteError
from nameko.rpc import rpc
from nameko.standalone.rpc import RpcProxy


class ExampleEvent(Event):
    type = "example"


entrypoint_called = Mock()


class Service(object):

    @rpc
    def echo(self, arg):
        entrypoint_called(arg)
        return arg

    @rpc
    def broken(self):
        return uuid.uuid4()  # does not serialize

    @event_handler('service', ExampleEvent.type)
    def event(self, evt_data):
        entrypoint_called(evt_data)


def test_rpc_serializationx(container_factory, rabbit_config):

    container = container_factory(Service, rabbit_config)
    container.start()

    data = {
        "hello": ("world",),
        123: 456,
        'abc': [7, 8, 9],
        'foobar': 1.5,
    }
    expected = {
        "hello": ["world", ],
        '123': 456,
        'abc': [7, 8, 9],
        'foobar': 1.5,
    }

    with RpcProxy('service', rabbit_config) as proxy:
        assert proxy.echo(data) == expected
        assert entrypoint_called.call_args == call(expected)


def test_rpc_result_serialization_error(container_factory, rabbit_config):

    container = container_factory(Service, rabbit_config)
    container.start()

    with RpcProxy('service', rabbit_config) as proxy:
        with pytest.raises(RemoteError) as exc:
            proxy.broken()
        assert exc.value.exc_type == "UnserializableValueError"

        assert proxy.echo('foo') == "foo"  # subsequent calls ok


def test_rpc_proxy_serialization_error(container_factory, rabbit_config):

    container = container_factory(Service, rabbit_config)
    container.start()

    with RpcProxy('service', rabbit_config) as proxy:
        with pytest.raises(TypeError):
            proxy.echo(uuid.uuid4())

        assert proxy.echo('foo') == "foo"  # subsequent calls ok
