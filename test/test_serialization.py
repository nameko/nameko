import json

from kombu.serialization import register
from mock import Mock, call
import pytest

from nameko.constants import SERIALIZER_CONFIG_KEY
from nameko.events import event_handler
from nameko.exceptions import RemoteError
from nameko.rpc import rpc
from nameko.standalone.rpc import ServiceRpcProxy


entrypoint_called = Mock()


test_data = {
    "hello": ("world",),
    123: 456,
    'abc': [7, 8, 9],
    'foobar': 1.5,
}

test_data_through_json = json.loads(json.dumps(test_data))


def unserializable():
    pass


class Service(object):
    name = "service"

    @rpc
    def echo(self, arg):
        entrypoint_called(arg)
        return arg

    @rpc
    def broken(self):
        return unserializable

    @event_handler('service', 'example')
    def event(self, evt_data):
        entrypoint_called(evt_data)


@pytest.mark.parametrize("serializer,data,expected", [
    ('json', test_data, test_data_through_json),
    ('pickle', test_data, test_data)])
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


def test_rpc_arg_serialization_error(container_factory, rabbit_config):

    container = container_factory(Service, rabbit_config)
    container.start()

    with ServiceRpcProxy('service', rabbit_config) as proxy:
        with pytest.raises(Exception):
            proxy.echo(unserializable)

        assert proxy.echo('foo') == "foo"  # subsequent calls ok


def test_custom_serializer(container_factory, rabbit_config, rabbit_manager):

    def encode(value):
        value = json.dumps(value)
        return value.upper()

    def decode(value):
        value = value.lower()
        return json.loads(value)

    register("upperjson", encode, decode, "application/x-upper-json", "utf-8")

    class Service(object):
        name = "service"

        @rpc
        def echo(self, arg):
            return arg

    rabbit_config[SERIALIZER_CONFIG_KEY] = "upperjson"
    container = container_factory(Service, rabbit_config)
    container.start()

    # create a queue to sniff RPC messages
    vhost = rabbit_config['vhost']
    rabbit_manager.create_queue(vhost, "rpc-sniffer", auto_delete=True)
    rabbit_manager.create_queue_binding(
        vhost, "nameko-rpc", "rpc-sniffer", routing_key="*")

    # verify RPC works end-to-end
    with ServiceRpcProxy('service', rabbit_config) as proxy:
        assert proxy.echo("hello") == "hello"

    # verify sniffed messages serialized as expected
    messages = rabbit_manager.get_messages(vhost, "rpc-sniffer")
    msg = messages[0]
    assert msg['payload'] == '{"RESULT": "HELLO", "ERROR": NULL}'
    assert msg['properties']['content_type'] == "application/x-upper-json"
