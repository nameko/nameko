import socket
import ssl

import pytest
from amqp.exceptions import AccessRefused, NotAllowed
from urllib3.util import Url, parse_url

from nameko.amqp import verify_amqp_uri


def test_good(rabbit_config):
    amqp_uri = rabbit_config['AMQP_URI']
    verify_amqp_uri(amqp_uri)


def test_bad_user(rabbit_config):
    scheme, auth, host, port, path, _, _ = parse_url(rabbit_config['AMQP_URI'])
    amqp_uri = Url(scheme, 'invalid:invalid', host, port, path).url

    with pytest.raises(AccessRefused) as exc_info:
        verify_amqp_uri(amqp_uri)
    message = str(exc_info.value)
    assert 'Login was refused' in message


def test_bad_vhost(rabbit_config):
    scheme, auth, host, port, path, _, _ = parse_url(rabbit_config['AMQP_URI'])
    amqp_uri = Url(scheme, auth, host, port, '/unknown').url

    with pytest.raises(NotAllowed) as exc_info:
        verify_amqp_uri(amqp_uri)
    message = str(exc_info.value)
    assert 'access to vhost' in message


def test_other_error(rabbit_config):
    scheme, auth, host, port, path, _, _ = parse_url(rabbit_config['AMQP_URI'])
    amqp_uri = Url(scheme, auth, host, port + 1, path).url  # closed port

    with pytest.raises(socket.error):
        verify_amqp_uri(amqp_uri)


def test_ssl_default_options(rabbit_ssl_config):
    amqp_ssl_uri = rabbit_ssl_config['AMQP_URI']
    verify_amqp_uri(amqp_ssl_uri, ssl=rabbit_ssl_config['AMQP_SSL'])


def test_ssl_no_options(rabbit_ssl_config):
    amqp_ssl_uri = rabbit_ssl_config['AMQP_URI']
    verify_amqp_uri(amqp_ssl_uri, ssl=True)


def test_ssl_non_default_option(rabbit_ssl_config):
    amqp_ssl_uri = rabbit_ssl_config['AMQP_URI']
    amqp_ssl_config = rabbit_ssl_config['AMQP_SSL']
    amqp_ssl_config['ssl_version'] = ssl.PROTOCOL_TLSv1_2

    verify_amqp_uri(amqp_ssl_uri, ssl=amqp_ssl_config)


def test_ssl_missing_option(rabbit_ssl_config):
    amqp_ssl_uri = rabbit_ssl_config['AMQP_URI']
    amqp_ssl_config = rabbit_ssl_config['AMQP_SSL']
    del amqp_ssl_config['keyfile']

    with pytest.raises(IOError) as exc_info:
        verify_amqp_uri(amqp_ssl_uri, ssl=amqp_ssl_config)

    message = str(exc_info.value)
    # without the private key we'll get a PEM lib error trying to connect
    assert 'PEM lib' in message


def test_ssl_to_wrong_port(rabbit_config, rabbit_ssl_config):
    amqp_ssl_uri = rabbit_config['AMQP_URI']  # not ssl uri

    with pytest.raises(IOError) as exc_info:
        verify_amqp_uri(amqp_ssl_uri, ssl=rabbit_ssl_config['AMQP_SSL'])

    message = str(exc_info.value)
    assert 'unknown protocol' in message
