import json
import uuid

import pytest
import yaml
from kombu import Exchange, Queue
from mock import Mock, call

from nameko.constants import (
    ACCEPT_CONFIG_KEY, SERIALIZER_CONFIG_KEY, SERIALIZERS_CONFIG_KEY
)
from nameko.events import EventDispatcher, event_handler
from nameko.exceptions import ConfigurationError, RemoteError
from nameko.messaging import consume
from nameko.rpc import RpcProxy, rpc
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


def upperjson_encode(value):
    value = json.dumps(value)
    return value.upper()


def upperjson_decode(value):
    value = value.lower()
    return json.loads(value)


def test_custom_serializer(container_factory, rabbit_config,
                           sniffer_queue_factory):

    class Service(object):
        name = "service"

        @rpc
        def echo(self, arg):
            return arg

    rabbit_config[SERIALIZER_CONFIG_KEY] = "upperjson"
    rabbit_config[SERIALIZERS_CONFIG_KEY] = {
        'upperjson': {
            'encoder': 'test.test_serialization.upperjson_encode',
            'decoder': 'test.test_serialization.upperjson_decode',
            'content_type': 'application/x-upper-json'
        }
    }
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


@pytest.mark.parametrize(
    'config',
    (
        {SERIALIZER_CONFIG_KEY: 'unknown'},
        {ACCEPT_CONFIG_KEY: ['json', 'unknown']},
        {
            SERIALIZER_CONFIG_KEY: 'json',
            ACCEPT_CONFIG_KEY: ['json', 'unknown'],
        },
    )
)
def test_missing_serializers(container_factory, rabbit_config, config):

    rabbit_config.update(config)
    with pytest.raises(ConfigurationError) as exc:
        container_factory(Service, rabbit_config)

    assert (
        str(exc.value) ==
        'Please register a serializer for "unknown" format')


@pytest.mark.parametrize(
    'content_type, encode',
    (
        ('application/json', json.dumps),
        ('application/x-yaml', yaml.dump),
    )
)
def test_consumer_accepts_multiple_serialization_formats(
    container_factory, rabbit_config, rabbit_manager, content_type, encode
):

    class Service(object):

        name = 'service'

        @consume(queue=Queue(exchange=Exchange('spam'), name='some-queue'))
        def consume(self, payload):
            assert payload == {'spam': 'ham'}

    rabbit_config[ACCEPT_CONFIG_KEY] = ['json', 'yaml']

    container = container_factory(Service, rabbit_config)
    container.start()

    payload = {'spam': 'ham'}

    with entrypoint_waiter(container, 'consume'):
        rabbit_manager.publish(
            rabbit_config['vhost'], 'spam', '', encode(payload),
            properties={'content_type': content_type})


@pytest.mark.parametrize(
    'serializer, content_type, encode',
    (
        ('json', 'application/json', json.dumps),
        ('yaml', 'application/x-yaml', yaml.dump),
    )
)
def test_standalone_rpc_accepts_multiple_serialization_formats(
    container_factory, rabbit_config, rabbit_manager,
    sniffer_queue_factory, serializer, content_type, encode
):

    called = Mock()

    class EchoingService(object):

        name = 'echoer'

        @rpc
        def echo(self, payload):
            called(payload)
            return payload

    echoer_config = rabbit_config.copy()
    echoer_config[SERIALIZER_CONFIG_KEY] = 'json'
    echoer_config[ACCEPT_CONFIG_KEY] = ['json', 'yaml']

    echoer = container_factory(EchoingService, echoer_config)
    echoer.start()

    payload = {'spam': 'ham'}

    proxy_config = rabbit_config.copy()
    proxy_config[SERIALIZER_CONFIG_KEY] = serializer

    get_messages = sniffer_queue_factory('nameko-rpc')

    with ServiceRpcProxy('echoer', proxy_config) as proxy:
        assert proxy.echo(payload) == payload

    msg = get_messages().pop()
    assert encode(payload) in msg['payload']
    assert msg['properties']['content_type'] == content_type

    assert called.mock_calls == [call(payload)]


@pytest.mark.parametrize(
    'serializer, content_type, encode',
    (
        ('json', 'application/json', json.dumps),
        ('yaml', 'application/x-yaml', yaml.dump),
    )
)
def test_rpc_accepts_multiple_serialization_formats(
    container_factory, rabbit_config, rabbit_manager,
    sniffer_queue_factory, serializer, content_type, encode
):

    called = Mock()

    class ForwardingService(object):

        name = 'forwarder'

        echoer = RpcProxy('echoer')

        @rpc
        def forward(self, payload):
            return self.echoer.echo(payload)

    class EchoingService(object):

        name = 'echoer'

        @rpc
        def echo(self, payload):
            called(payload)
            return payload

    echoer_config = rabbit_config.copy()
    echoer_config[SERIALIZER_CONFIG_KEY] = 'json'
    echoer_config[ACCEPT_CONFIG_KEY] = ['json', 'yaml']

    echoer = container_factory(EchoingService, echoer_config)
    echoer.start()

    payload = {'spam': 'ham'}

    forwarder_config = rabbit_config.copy()
    forwarder_config[SERIALIZER_CONFIG_KEY] = serializer
    forwarder = container_factory(ForwardingService, forwarder_config)
    forwarder.start()

    get_messages = sniffer_queue_factory('nameko-rpc')

    with entrypoint_hook(forwarder, 'forward') as echo:
        assert echo(payload) == payload

    msg = get_messages().pop()
    assert encode(payload) in msg['payload']
    assert msg['properties']['content_type'] == content_type

    assert called.mock_calls == [call(payload)]


@pytest.mark.parametrize(
    'serializer, content_type, encode',
    (
        ('json', 'application/json', json.dumps),
        ('yaml', 'application/x-yaml', yaml.dump),
    )
)
def test_events_accepts_multiple_serialization_formats(
    container_factory, rabbit_config, rabbit_manager,
    sniffer_queue_factory, serializer, content_type, encode
):

    called = Mock()

    class PublishingService(object):

        name = 'publisher'

        dispatch = EventDispatcher()

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

    consumer_config = rabbit_config.copy()
    consumer_config[SERIALIZER_CONFIG_KEY] = 'json'
    consumer_config[ACCEPT_CONFIG_KEY] = ['json', 'yaml']

    consumer = container_factory(ConsumingService, consumer_config)
    consumer.start()

    get_messages = sniffer_queue_factory(
        'publisher.events', routing_key='spam')

    payload = {'spam': 'ham'}

    publisher_config = rabbit_config.copy()
    publisher_config[SERIALIZER_CONFIG_KEY] = serializer
    publisher = container_factory(PublishingService, publisher_config)
    publisher.start()
    publish(consumer, publisher)

    msg = get_messages().pop()
    assert encode(payload) in msg['payload']
    assert msg['properties']['content_type'] == content_type

    assert called.mock_calls == [call(payload)]
