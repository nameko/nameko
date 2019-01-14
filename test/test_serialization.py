import json
import uuid

import pytest
import yaml
from kombu import Exchange, Queue
from mock import Mock, call

from nameko import config
from nameko.constants import (
    ACCEPT_CONFIG_KEY, SERIALIZER_CONFIG_KEY, SERIALIZERS_CONFIG_KEY
)
from nameko.events import EventDispatcher, event_handler
from nameko.exceptions import ConfigurationError, RemoteError
from nameko.messaging import consume
from nameko.rpc import ServiceRpc, rpc
from nameko.standalone.rpc import ServiceRpcClient
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

unserializable = object()


class Service(object):
    name = "service"

    @rpc
    def echo(self, arg):
        entrypoint_called(arg)
        return arg

    @rpc
    def broken(self):
        return unserializable


@pytest.fixture
def sniffer_queue_factory(rabbit_config, rabbit_manager, get_vhost):
    """ Return a function that creates message queues to 'sniff' messages
    published to exchanges.
    """
    vhost = get_vhost(config['AMQP_URI'])

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


@pytest.mark.usefixtures("rabbit_config")
@pytest.mark.parametrize("serializer", ['json', 'pickle'])
def test_rpc_serialization(container_factory, sniffer_queue_factory, serializer):

    with config.patch({SERIALIZER_CONFIG_KEY: serializer}):
        container = container_factory(Service)
        container.start()

        get_messages = sniffer_queue_factory('nameko-rpc')

        serialized = serialized_info[serializer]

        with ServiceRpcClient('service') as client:
            assert client.echo(test_data) == serialized['data']
            assert entrypoint_called.call_args == call(serialized['data'])

    msg = get_messages()[0]
    assert msg['properties']['content_type'] == serialized['content_type']


@pytest.mark.usefixtures("rabbit_config")
def test_rpc_result_serialization_error(container_factory):

    container = container_factory(Service)
    container.start()

    with ServiceRpcClient('service') as client:
        with pytest.raises(RemoteError) as exc:
            client.broken()
        assert exc.value.exc_type == "UnserializableValueError"

        assert client.echo('foo') == "foo"  # subsequent calls ok


@pytest.mark.usefixtures("rabbit_config")
def test_rpc_arg_serialization_error(container_factory):

    container = container_factory(Service)
    container.start()

    with ServiceRpcClient('service') as client:
        with pytest.raises(Exception):
            client.echo(unserializable)

        assert client.echo('foo') == "foo"  # subsequent calls ok


@pytest.mark.usefixtures("rabbit_config")
@pytest.mark.parametrize("serializer", ['json', 'pickle'])
def test_event_serialization(
    container_factory, sniffer_queue_factory, serializer
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

    with config.patch({SERIALIZER_CONFIG_KEY: serializer}):

        container = container_factory(Service)
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


def upperjson_encode(value):
    value = json.dumps(value)
    return value.upper()


def upperjson_decode(value):
    value = value.lower()
    return json.loads(value)


@pytest.mark.usefixtures("rabbit_config")
def test_custom_serializer(container_factory, sniffer_queue_factory):

    class Service(object):
        name = "service"

        @rpc
        def echo(self, arg):
            return arg

    with config.patch({
        SERIALIZER_CONFIG_KEY: "upperjson",
        SERIALIZERS_CONFIG_KEY: {
            'upperjson': {
                'encoder': 'test.test_serialization.upperjson_encode',
                'decoder': 'test.test_serialization.upperjson_decode',
                'content_type': 'application/x-upper-json'
            }
        }
    }):
        container = container_factory(Service)
        container.start()

        get_messages = sniffer_queue_factory('nameko-rpc')

        # verify RPC works end-to-end
        with ServiceRpcClient('service') as client:
            assert client.echo("hello") == "hello"

    # verify sniffed messages serialized as expected
    msg = get_messages()[0]
    assert '"RESULT": "HELLO"' in msg['payload']
    assert msg['properties']['content_type'] == "application/x-upper-json"


@pytest.mark.usefixtures("rabbit_config")
@pytest.mark.parametrize(
    'cfg',
    (
        {SERIALIZER_CONFIG_KEY: 'unknown'},
        {ACCEPT_CONFIG_KEY: ['json', 'unknown']},
        {
            SERIALIZER_CONFIG_KEY: 'json',
            ACCEPT_CONFIG_KEY: ['json', 'unknown'],
        },
    )
)
def test_missing_serializers(container_factory, cfg):

    with config.patch(cfg):
        with pytest.raises(ConfigurationError) as exc:
            container_factory(Service)

    assert (
        str(exc.value) ==
        'Please register a serializer for "unknown" format')


@pytest.yield_fixture
def multi_serializer_config():
    with config.patch({ACCEPT_CONFIG_KEY: ['json', 'yaml']}):
        yield


@pytest.mark.usefixtures("rabbit_config")
@pytest.mark.usefixtures("multi_serializer_config")
@pytest.mark.parametrize(
    'content_type, encode',
    (
        ('application/json', json.dumps),
        ('application/x-yaml', yaml.dump),
    )
)
def test_consumer_accepts_multiple_serialization_formats(
    container_factory, rabbit_manager, content_type, encode, get_vhost
):

    class Service(object):

        name = 'service'

        @consume(queue=Queue(exchange=Exchange('spam'), name='some-queue'))
        def consume(self, payload):
            assert payload == {'spam': 'ham'}

    container = container_factory(Service)
    container.start()

    payload = {'spam': 'ham'}

    with entrypoint_waiter(container, 'consume'):
        rabbit_manager.publish(
            get_vhost(config['AMQP_URI']), 'spam', '', encode(payload),
            properties={'content_type': content_type})


@pytest.mark.usefixtures("rabbit_config")
@pytest.mark.usefixtures("multi_serializer_config")
@pytest.mark.parametrize(
    'serializer, content_type, encode',
    (
        ('json', 'application/json', json.dumps),
        ('yaml', 'application/x-yaml', yaml.dump),
    )
)
def test_standalone_rpc_accepts_multiple_serialization_formats(
    container_factory, sniffer_queue_factory, serializer,
    content_type, encode
):

    called = Mock()

    class EchoingService(object):

        name = 'echoer'

        @rpc
        def echo(self, payload):
            called(payload)
            return payload

    echoer = container_factory(EchoingService)
    echoer.start()

    payload = {'spam': 'ham'}

    get_messages = sniffer_queue_factory('nameko-rpc')

    with ServiceRpcClient('echoer', serializer=serializer) as client:
        assert client.echo(payload) == payload

    msg = get_messages().pop()
    assert encode(payload) in msg['payload']
    assert msg['properties']['content_type'] == content_type

    assert called.mock_calls == [call(payload)]


@pytest.mark.usefixtures("rabbit_config")
@pytest.mark.usefixtures("multi_serializer_config")
@pytest.mark.parametrize(
    'serializer, content_type, encode',
    (
        ('json', 'application/json', json.dumps),
        ('yaml', 'application/x-yaml', yaml.dump),
    )
)
def test_rpc_accepts_multiple_serialization_formats(
    container_factory, rabbit_manager, sniffer_queue_factory,
    serializer, content_type, encode
):

    called = Mock()

    class ForwardingService(object):

        name = 'forwarder'

        echoer = ServiceRpc('echoer', serializer=serializer)

        @rpc
        def forward(self, payload):
            return self.echoer.echo(payload)

    class EchoingService(object):

        name = 'echoer'

        @rpc
        def echo(self, payload):
            called(payload)
            return payload

    echoer = container_factory(EchoingService)
    echoer.start()

    payload = {'spam': 'ham'}

    forwarder = container_factory(ForwardingService)
    forwarder.start()

    get_messages = sniffer_queue_factory('nameko-rpc')

    with entrypoint_hook(forwarder, 'forward') as echo:
        assert echo(payload) == payload

    msg = get_messages().pop()
    assert encode(payload) in msg['payload']
    assert msg['properties']['content_type'] == content_type

    assert called.mock_calls == [call(payload)]


@pytest.mark.usefixtures("rabbit_config")
@pytest.mark.usefixtures("multi_serializer_config")
@pytest.mark.parametrize(
    'serializer, content_type, encode',
    (
        ('json', 'application/json', json.dumps),
        ('yaml', 'application/x-yaml', yaml.dump),
    )
)
def test_events_accepts_multiple_serialization_formats(
    container_factory, rabbit_manager, sniffer_queue_factory,
    serializer, content_type, encode
):

    called = Mock()

    class PublishingService(object):

        name = 'publisher'

        dispatch = EventDispatcher(serializer=serializer)

        @rpc
        def dispatch_event(self, payload):
            self.dispatch('spam', payload)

    class ConsumingService(object):

        name = 'consumer'

        @event_handler('publisher', 'spam')
        def handle_event(self, event_data):
            called(event_data)

    def publish(consumer, publisher):
        with entrypoint_waiter(consumer, "handle_event"):
            with entrypoint_hook(publisher, "dispatch_event") as dispatch:
                dispatch(payload)

    consumer = container_factory(ConsumingService)
    consumer.start()

    get_messages = sniffer_queue_factory(
        'publisher.events', routing_key='spam')

    payload = {'spam': 'ham'}

    publisher = container_factory(PublishingService)
    publisher.start()

    publish(consumer, publisher)

    msg = get_messages().pop()
    assert encode(payload) in msg['payload']
    assert msg['properties']['content_type'] == content_type

    assert called.mock_calls == [call(payload)]
