import socket

import pytest
from urllib3.util import Url, parse_url

from nameko.amqp import verify_amqp_uri


@pytest.fixture
def uris(request, rabbit_config):
    amqp_uri = rabbit_config['AMQP_URI']
    scheme, auth, host, port, path, _, _ = parse_url(amqp_uri)
    bad_port = Url(scheme, auth, host, port + 1, path).url
    bad_user = Url(scheme, 'invalid:invalid', host, port, path).url
    bad_vhost = Url(scheme, auth, host, port, '/unknown').url
    ssl_port = request.config.getoption('AMQP_SSL_PORT')
    amqp_ssl_uri = Url(scheme, auth, host, ssl_port, path).url
    return {
        'good': amqp_uri,
        'bad_port': bad_port,
        'bad_user': bad_user,
        'bad_vhost': bad_vhost,
        'ssl': amqp_ssl_uri,
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


def test_ssl_good(uris, rabbit_ssl_config):
    ssl_uri = uris['ssl']
    verify_amqp_uri(ssl_uri, ssl=rabbit_ssl_config['AMQP_SSL'])


def test_ssl_bad_port(uris, rabbit_ssl_config):
    amqp_uri = uris['good']

    with pytest.raises(IOError) as exc_info:
        verify_amqp_uri(amqp_uri, ssl=rabbit_ssl_config['AMQP_SSL'])

    message = str(exc_info.value)

    assert 'unknown protocol' in message


def test_ssl_missing_param(uris, rabbit_ssl_config):
    ssl_uri = uris['ssl']
    amqp_ssl_config = rabbit_ssl_config['AMQP_SSL']

    del amqp_ssl_config['keyfile']

    with pytest.raises(IOError) as exc_info:
        verify_amqp_uri(ssl_uri, ssl=amqp_ssl_config)

    message = str(exc_info.value)

    assert 'PEM lib' in message
