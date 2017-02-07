from __future__ import absolute_import

from amqp.exceptions import NotFound
from kombu import Connection
from kombu.messaging import Producer
import pytest

from nameko.amqp.publish import get_connection, get_producer, Publisher


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
