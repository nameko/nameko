import socket

import pytest
from urllib3.util import parse_url, Url

from nameko.amqp import verify_amqp_uri


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
