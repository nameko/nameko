from mock import Mock, call
import pytest

from nameko.events import event_handler
from nameko.exceptions import RemoteError
from nameko.rpc import rpc
from nameko.standalone.rpc import ServiceRpcProxy
from nameko.constants import SERIALIZER_CONFIG_KEY


entrypoint_called = Mock()


@pytest.yield_fixture
def unserializable():
    def unserializable_inner():
        pass
    yield unserializable_inner


@pytest.fixture
def test_data():
    return {"hello": ("world",),
            123: 456,
            'abc': [7, 8, 9],
            'foobar': 1.5, }


@pytest.fixture
def test_data_through_json():
    return {"hello": ["world", ],
            '123': 456,
            'abc': [7, 8, 9],
            'foobar': 1.5, }


class Service(object):
    name = "service"

    @rpc
    def echo(self, arg):
        entrypoint_called(arg)
        return arg

    @rpc
    def broken(self):
        return unserializable()  # does not serialize

    @event_handler('service', 'example')
    def event(self, evt_data):
        entrypoint_called(evt_data)


@pytest.mark.parametrize("serializer,data,expected", [
    ('json', test_data(), test_data_through_json()),
    ('pickle', test_data(), test_data())])
def test_rpc_serialization(container_factory, rabbit_config,
                           serializer, data, expected):

    config = rabbit_config
    config[SERIALIZER_CONFIG_KEY] = serializer
    container = container_factory(Service, config)
    container.start()

    with ServiceRpcProxy('service', rabbit_config) as proxy:
        assert proxy.echo(data) == expected
        assert entrypoint_called.call_args == call(expected)


def test_rpc_result_serialization_error(container_factory, rabbit_config):

    container = container_factory(Service, rabbit_config)
    container.start()

    with ServiceRpcProxy('service', rabbit_config) as proxy:
        with pytest.raises(RemoteError) as exc:
            proxy.broken()
        assert exc.value.exc_type == "UnserializableValueError"

        assert proxy.echo('foo') == "foo"  # subsequent calls ok


def test_rpc_proxy_serialization_error(
        container_factory, rabbit_config, unserializable):

    container = container_factory(Service, rabbit_config)
    container.start()

    with ServiceRpcProxy('service', rabbit_config) as proxy:
        with pytest.raises(Exception):
            proxy.echo(unserializable)

        assert proxy.echo('foo') == "foo"  # subsequent calls ok
