from __future__ import absolute_import

from time import time

import pytest
from amqp.exceptions import (
    NotFound, PreconditionFailed, RecoverableConnectionError
)
from kombu import Connection
from kombu.common import maybe_declare
from kombu.compression import get_encoder
from kombu.exceptions import OperationalError
from kombu.messaging import Exchange, Producer, Queue
from kombu.serialization import registry
from mock import ANY, MagicMock, Mock, call, patch
from six.moves import queue

from nameko.amqp.publish import (
    Publisher, UndeliverableMessage, get_connection, get_producer
)


def test_get_connection(rabbit_config):
    amqp_uri = rabbit_config['AMQP_URI']
    connection_ids = []

    with get_connection(amqp_uri) as connection:
        connection_ids.append(id(connection))
        assert isinstance(connection, Connection)

    with get_connection(amqp_uri) as connection:
        connection_ids.append(id(connection))
        assert len(set(connection_ids)) == 1


class TestGetProducer(object):

    @pytest.fixture(params=[True, False])
    def confirms(self, request):
        return request.param

    def test_get_producer(self, rabbit_config, confirms):
        amqp_uri = rabbit_config['AMQP_URI']
        producer_ids = []

        with get_producer(amqp_uri, confirms) as producer:
            producer_ids.append(id(producer))
            transport_options = producer.connection.transport_options
            assert isinstance(producer, Producer)
            assert transport_options['confirm_publish'] is confirms

        with get_producer(amqp_uri, confirms) as producer:
            producer_ids.append(id(producer))
            assert len(set(producer_ids)) == 1

    def test_pool_gives_different_producers(self, rabbit_config):
        amqp_uri = rabbit_config['AMQP_URI']
        producer_ids = []

        # get a producer
        with get_producer(amqp_uri, True) as confirmed_producer:
            producer_ids.append(id(confirmed_producer))
            assert len(set(producer_ids)) == 1

        # get a producer with the same parameters
        with get_producer(amqp_uri, True) as confirmed_producer:
            producer_ids.append(id(confirmed_producer))
            assert len(set(producer_ids)) == 1  # same producer returned

        # get a producer with different parameters
        with get_producer(amqp_uri, False) as unconfirmed_producer:
            producer_ids.append(id(unconfirmed_producer))
            assert len(set(producer_ids)) == 2  # different producer returned


class TestPublisherConfirms(object):
    """ Publishing to a non-existent exchange raises if confirms are enabled.
    """

    def test_confirms_disabled(self, rabbit_config):
        amqp_uri = rabbit_config['AMQP_URI']

        with get_producer(amqp_uri, False) as producer:
            producer.publish(
                "msg", exchange="missing", routing_key="key"
            )

    def test_confirms_enabled(self, rabbit_config):
        amqp_uri = rabbit_config['AMQP_URI']

        with pytest.raises(NotFound):
            with get_producer(amqp_uri) as producer:
                producer.publish(
                    "msg", exchange="missing", routing_key="key"
                )


@pytest.mark.behavioural
class TestPublisher(object):

    @pytest.fixture
    def routing_key(self):
        return "routing_key"

    @pytest.fixture
    def exchange(self, amqp_uri):
        exchange = Exchange(name="exchange")
        return exchange

    @pytest.fixture
    def queue(self, amqp_uri, exchange, routing_key):
        queue = Queue(
            name="queue", exchange=exchange, routing_key=routing_key
        )
        return queue

    @pytest.fixture
    def publisher(self, amqp_uri, exchange, queue, routing_key):
        return Publisher(
            amqp_uri,
            serializer="json",
            exchange=exchange,
            routing_key=routing_key,
            declare=[exchange, queue]
        )

    def test_routing(
        self, publisher, get_message_from_queue, queue
    ):
        # exchange and routing key can be specified at publish time
        publisher.publish(
            "payload", exchange=None, routing_key=queue.name
        )
        message = get_message_from_queue(queue.name)
        assert message.delivery_info['routing_key'] == queue.name

    @pytest.mark.parametrize("option,value,expected", [
        ('delivery_mode', 1, 1),
        ('priority', 10, 10),
        ('expiration', 10, str(10 * 1000)),
    ])
    def test_delivery_options(
        self, publisher, get_message_from_queue, queue, option, value, expected
    ):
        publisher.publish("payload", **{option: value})
        message = get_message_from_queue(queue.name)
        assert message.properties[option] == expected

    @pytest.mark.filterwarnings("ignore:Mandatory delivery:UserWarning")
    def test_confirms(self, amqp_uri, publisher):
        publisher.mandatory = True

        # confirms are enabled by default;
        # if a mandatory message cannot be delivered, expect an exception
        with pytest.raises(UndeliverableMessage):
            publisher.publish(
                "payload", routing_key="missing", use_confirms=True
            )

        # disabling confirms disables the error
        publisher.publish(
            "payload", routing_key="missing", use_confirms=False
        )

    def test_mandatory_delivery(
        self, publisher, get_message_from_queue, queue
    ):
        # messages are not mandatory by default;
        # no error when routing to a non-existent queue
        publisher.publish("payload", routing_key="missing", mandatory=False)

        # requesting mandatory delivery will result in an exception
        # if there is no bound queue to receive the message
        with pytest.raises(UndeliverableMessage):
            publisher.publish("payload", routing_key="missing", mandatory=True)

        # no exception will be raised if confirms are disabled,
        # even when mandatory delivery is requested,
        # but there will be a warning raised
        with patch('nameko.amqp.publish.warnings') as warnings:
            publisher.publish(
                "payload",
                routing_key="missing",
                mandatory=True,
                use_confirms=False
            )
        assert warnings.warn.called

    @pytest.mark.parametrize("serializer", ['json', 'pickle'])
    def test_serializer(
        self, publisher, get_message_from_queue, queue, serializer
    ):
        payload = {"key": "value"}
        publisher.publish(payload, serializer=serializer)

        content_type, content_encoding, expected_body = (
            registry.dumps(payload, serializer=serializer)
        )

        message = get_message_from_queue(queue.name, accept=content_type)
        assert message.body == expected_body

    def test_compression(
        self, publisher, get_message_from_queue, queue
    ):
        payload = {"key": "value"}
        publisher.publish(payload, compression="gzip")

        _, expected_content_type = get_encoder('gzip')

        message = get_message_from_queue(queue.name)
        assert message.headers['compression'] == expected_content_type

    def test_content_type(
        self, publisher, get_message_from_queue, queue
    ):
        payload = (
            b'GIF89a\x01\x00\x01\x00\x00\xff\x00,'
            b'\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x00;'
        )
        publisher.publish(payload, content_type="image/gif")

        message = get_message_from_queue(queue.name)
        assert message.payload == payload
        assert message.properties['content_type'] == 'image/gif'
        assert message.properties['content_encoding'] == 'binary'

    def test_content_encoding(
        self, publisher, get_message_from_queue, queue
    ):
        payload = "A"
        publisher.publish(
            payload.encode('utf-16'),
            content_type="application/text",
            content_encoding="utf-16"
        )

        message = get_message_from_queue(queue.name)
        assert message.payload == payload
        assert message.properties['content_type'] == 'application/text'
        assert message.properties['content_encoding'] == 'utf-16'

    @pytest.mark.parametrize("headers,extra_headers,expected_headers", [
        # no extra headers
        ({'x-foo': 'foo'}, {}, {'x-foo': 'foo'}),
        # only extra headers
        ({}, {'x-foo': 'foo'}, {'x-foo': 'foo'}),
        # both
        ({'x-foo': 'foo'}, {'x-bar': 'bar'}, {'x-foo': 'foo', 'x-bar': 'bar'}),
        # override
        ({'x-foo': 'foo'}, {'x-foo': 'bar'}, {'x-foo': 'bar'}),
    ])
    @pytest.mark.usefixtures('predictable_call_ids')
    def test_headers(
        self, publisher, get_message_from_queue, queue,
        headers, extra_headers, expected_headers
    ):
        payload = {"key": "value"}

        publisher.publish(
            payload, headers=headers, extra_headers=extra_headers
        )

        message = get_message_from_queue(queue.name)
        assert message.headers == expected_headers
        assert message.properties['application_headers'] == expected_headers

    @pytest.mark.parametrize("option,value,expected", [
        ('reply_to', "queue_name", "queue_name"),
        ('message_id', "msg.1", "msg.1"),
        ('type', "type", "type"),
        ('app_id', "app.1", "app.1"),
        ('cluster_id', "cluster.1", "cluster.1"),
        ('correlation_id', "msg.1", "msg.1"),
    ])
    def test_message_properties(
        self, publisher, get_message_from_queue, queue, option, value, expected
    ):
        publisher.publish("payload", **{option: value})

        message = get_message_from_queue(queue.name)
        assert message.properties[option] == expected

    def test_timestamp(
        self, publisher, get_message_from_queue, queue
    ):
        now = int(time())
        publisher.publish("payload", timestamp=now)

        message = get_message_from_queue(queue.name)
        assert message.properties['timestamp'] == now

    def test_user_id(
        self, publisher, get_message_from_queue, queue, rabbit_config
    ):
        user_id = rabbit_config['username']

        # successful case
        publisher.publish("payload", user_id=user_id)

        message = get_message_from_queue(queue.name)
        assert message.properties['user_id'] == user_id

        # when user_id does not match the current user, expect an error
        with pytest.raises(PreconditionFailed):
            publisher.publish("payload", user_id="invalid")

    @patch('kombu.messaging.maybe_declare', wraps=maybe_declare)
    def test_declare(
        self, maybe_declare, publisher, get_message_from_queue, routing_key,
        queue, exchange
    ):
        declare = [
            Queue(name="q1", exchange=exchange, routing_key=routing_key),
            Queue(name="q2", exchange=exchange, routing_key=routing_key)
        ]

        publisher.publish("payload", declare=declare)

        assert maybe_declare.call_args_list == [
            call(exchange, ANY, ANY),
            call(queue, ANY, ANY),
            call(declare[0], ANY, ANY),
            call(declare[1], ANY, ANY)
        ]

        assert get_message_from_queue(queue.name).payload == "payload"
        assert get_message_from_queue(declare[0].name).payload == "payload"
        assert get_message_from_queue(declare[1].name).payload == "payload"

    def test_retry(
        self, publisher, get_message_from_queue, rabbit_config
    ):
        mock_publish = MagicMock(__name__="", __doc__="", __module__="")
        mock_publish.side_effect = RecoverableConnectionError("error")

        expected_retries = publisher.retry_policy['max_retries'] + 1

        # with retry
        with patch.object(Producer, '_publish', new=mock_publish):
            with pytest.raises(OperationalError):
                publisher.publish("payload", retry=True)
        assert mock_publish.call_count == 1 + expected_retries

        mock_publish.reset_mock()

        # retry disabled
        with patch.object(Producer, '_publish', new=mock_publish):
            with pytest.raises(RecoverableConnectionError):
                publisher.publish("payload", retry=False)
        assert mock_publish.call_count == 1

    def test_retry_policy(
        self, publisher, get_message_from_queue, rabbit_config
    ):
        mock_publish = MagicMock(__name__="", __doc__="", __module__="")
        mock_publish.side_effect = RecoverableConnectionError("error")

        retry_policy = {
            'max_retries': 5
        }
        expected_retries = retry_policy['max_retries'] + 1

        with patch.object(Producer, '_publish', new=mock_publish):
            with pytest.raises(OperationalError):
                publisher.publish("payload", retry_policy=retry_policy)
        assert mock_publish.call_count == 1 + expected_retries


class TestDefaults(object):

    @pytest.yield_fixture
    def get_producer(self):
        with patch('nameko.amqp.publish.get_producer') as get_producer:
            yield get_producer

    @pytest.fixture
    def producer(self, get_producer):
        producer = get_producer().__enter__.return_value
        # make sure we don't raise UndeliverableMessage if mandatory is True
        producer.channel.returned_messages.get_nowait.side_effect = queue.Empty
        return producer

    @pytest.mark.parametrize("param", [
        # delivery options
        'delivery_mode', 'mandatory', 'priority', 'expiration',
        # message options
        'serializer', 'compression',
        # retry policy
        'retry', 'retry_policy'
    ])
    def test_precedence(self, param, producer):
        """ Verify that a default specified as a class attribute can be
        overriden by a default specified at instantiation time, which can
        further be overriden by a value specified when used.
        """
        publisher_cls = type("Publisher", (Publisher,), {param: "value"})
        publisher = publisher_cls("memory://", **{param: True})

        publisher.publish("payload")
        assert producer.publish.call_args[1][param] is True

        publisher.publish("payload", **{param: False})
        assert producer.publish.call_args[1][param] is False

    def test_header_precedence(self, producer):
        """ Verify that headers at publish time extend any provided
        at instantiation time.
        """
        headers1 = {'h1': Mock()}
        publisher = Publisher("memory://", headers=headers1)

        headers2 = {'h2': Mock()}
        publisher.publish("payload", headers=headers2)

        combined_headers = headers1.copy()
        combined_headers.update(headers2)
        assert producer.publish.call_args[1]["headers"] == combined_headers

        headers3 = {'h3': Mock()}
        publisher.publish("payload", headers=headers3)

        combined_headers = headers1.copy()
        combined_headers.update(headers3)
        assert producer.publish.call_args[1]["headers"] == combined_headers

    def test_declaration_precedence(self, producer):
        """ Verify that declarations at publish time extend any provided
        at instantiation time.
        """
        queue1 = Mock()
        publisher = Publisher("memory://", declare=[queue1])

        queue2 = Mock()
        publisher.publish("payload", declare=[queue2])
        assert producer.publish.call_args[1]["declare"] == [queue1, queue2]

        queue3 = Mock()
        publisher.publish("payload", declare=[queue3])
        assert producer.publish.call_args[1]["declare"] == [queue1, queue3]

    def test_publish_kwargs(self, producer):
        """ Verify that publish_kwargs at publish time augment any provided
        at instantiation time. Verify that publish_kwargs at publish time
        override any provided at instantiation time in the case of a clash.
        Verify that any keyword argument is transparently passed to kombu.
        """
        publisher = Publisher("memory://", reply_to="queue1")
        publisher.publish(
            "payload", reply_to="queue2", correlation_id="1", bogus="bogus"
        )

        # publish-time kwargs override instantiation-time kwargs
        assert producer.publish.call_args[1]["reply_to"] == "queue2"
        # publish-time kwargs augment instantiation-time kwargs
        assert producer.publish.call_args[1]["correlation_id"] == "1"
        # irrelevant keywords pass through transparently
        assert producer.publish.call_args[1]["bogus"] == "bogus"

    def test_use_confirms(self, get_producer):
        """ Verify that publish-confirms can be set as a default specified at
        instantiation time, which can be overriden by a value specified at
        publish time.
        """
        publisher = Publisher("memory://", use_confirms=False)

        publisher.publish("payload")
        use_confirms = get_producer.call_args[0][3].get('confirm_publish')
        assert use_confirms is False

        publisher.publish("payload", use_confirms=True)
        use_confirms = get_producer.call_args[0][3].get('confirm_publish')
        assert use_confirms is True
