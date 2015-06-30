import json
import uuid

from kombu.serialization import register
from mock import Mock, call
import pytest

from nameko.constants import SERIALIZER_CONFIG_KEY
from nameko.events import event_handler, EventDispatcher
from nameko.exceptions import RemoteError
from nameko.rpc import rpc
from nameko.standalone.rpc import ServiceRpcProxy
from nameko.testing.services import entrypoint_hook, entrypoint_waiter


entrypoint_called = Mock()


test_data = {
    "hello": ("world",),
    123: 456,
    'abc': [7, 8, 9],
    'foobar': 1.5,
}

serialized_info = {
    'json': {
        'content_type': 'application/json',
        'content_encoding': 'utf-8',
        'data': json.loads(json.dumps(test_data))
    },
    'pickle': {
        'content_type': 'application/x-python-serialize',
        'content_encoding': 'binary',
        'data': test_data
    }
}


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


@pytest.fixture
def sniffer_queue_factory(rabbit_config, rabbit_manager):
    """ Return a function that creates message queues to 'sniff' messages
    published to exchanges.
    """
    vhost = rabbit_config['vhost']

    def make(exchange, routing_key="*"):
        """ Create a uniquely named queue and bind it to an exchange so that
        it collects messages published to that exchange.

        :Parameters:
            exchange : str
                Name of the exchange to bind to
            routing_key : str
                Routing key to bind with

        :Returns:
            A function that returns all the messages received by the queue

        """
        queue_name = "sniffer_{}".format(uuid.uuid4())
        rabbit_manager.create_queue(vhost, queue_name, auto_delete=True)
        rabbit_manager.create_queue_binding(
            vhost, exchange, queue_name, routing_key=routing_key)

        def get_messages():
            """ Return all messages received by the sniffer queue and remove
            them from the queue.
            """
            return rabbit_manager.get_messages(vhost, queue_name)
        return get_messages

    return make


@pytest.mark.parametrize("serializer", ['json', 'pickle'])
def test_rpc_serialization(container_factory, rabbit_config,
                           sniffer_queue_factory, serializer):

    config = rabbit_config
    config[SERIALIZER_CONFIG_KEY] = serializer
    container = container_factory(Service, config)
    container.start()

    get_messages = sniffer_queue_factory('nameko-rpc')

    serialized = serialized_info[serializer]

    with ServiceRpcProxy('service', rabbit_config) as proxy:
        assert proxy.echo(test_data) == serialized['data']
        assert entrypoint_called.call_args == call(serialized['data'])

    msg = get_messages()[0]
    assert msg['properties']['content_type'] == serialized['content_type']


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


@pytest.mark.parametrize("serializer", ['json', 'pickle'])
def test_event_serialization(
    container_factory, rabbit_config, sniffer_queue_factory, serializer
):
    handler_called = Mock()

    class Service(object):
        name = "srcservice"
        dispatch = EventDispatcher()

        @rpc
        def dispatch_event(self, payload):
            self.dispatch("eventtype", payload)

        @event_handler('srcservice', 'eventtype')
        def handle_event(self, event_data):
            handler_called(event_data)

    rabbit_config[SERIALIZER_CONFIG_KEY] = serializer
    container = container_factory(Service, rabbit_config)
    container.start()

    get_messages = sniffer_queue_factory(
        "srcservice.events", routing_key="eventtype")

    serialized = serialized_info[serializer]

    # dispatch an event with a tuple payload
    with entrypoint_waiter(container, "handle_event"):
        with entrypoint_hook(container, "dispatch_event") as dispatch_event:
            dispatch_event(test_data)

    # verify data serialized to expected value
    assert handler_called.call_args == call(serialized['data'])

    # verify sniffed messages serialized as expected
    msg = get_messages()[0]
    assert msg['properties']['content_type'] == serialized['content_type']


def test_custom_serializer(container_factory, rabbit_config,
                           sniffer_queue_factory):

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

    get_messages = sniffer_queue_factory('nameko-rpc')

    # verify RPC works end-to-end
    with ServiceRpcProxy('service', rabbit_config) as proxy:
        assert proxy.echo("hello") == "hello"

    # verify sniffed messages serialized as expected
    msg = get_messages()[0]
    assert '"RESULT": "HELLO"' in msg['payload']
    assert msg['properties']['content_type'] == "application/x-upper-json"
