import socket

import pytest
from amqp.exceptions import NotFound
from kombu import Connection
from kombu.messaging import Producer
from urllib3.util import Url, parse_url

from nameko.amqp import get_connection, get_producer, verify_amqp_uri


@pytest.fixture
def uris(rabbit_config):
    amqp_uri = rabbit_config['AMQP_URI']
    scheme, auth, host, port, path, _, _ = parse_url(amqp_uri)
    bad_port = Url(scheme, auth, host, port + 1, path).url
    bad_user = Url(scheme, 'invalid:invalid', host, port, path).url
    bad_vhost = Url(scheme, auth, host, port, '/unknown').url
    return {
        'good': amqp_uri,
        'bad_port': bad_port,
        'bad_user': bad_user,
        'bad_vhost': bad_vhost,
    }


def test_good(uris):
    amqp_uri = uris['good']
    verify_amqp_uri(amqp_uri)


def test_bad_user(uris):
    amqp_uri = uris['bad_user']
    with pytest.raises(IOError) as exc_info:
        verify_amqp_uri(amqp_uri)
    message = str(exc_info.value)
    assert 'Error connecting to broker' in message
    assert 'invalid credentials' in message


def test_bad_vhost(uris):
    amqp_uri = uris['bad_vhost']
    with pytest.raises(IOError) as exc_info:
        verify_amqp_uri(amqp_uri)
    message = str(exc_info.value)
    assert 'Error connecting to broker' in message
    assert 'invalid or unauthorized vhost' in message


def test_other_error(uris):
    # other errors bubble
    amqp_uri = uris['bad_port']
    with pytest.raises(socket.error):
        verify_amqp_uri(amqp_uri)


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
