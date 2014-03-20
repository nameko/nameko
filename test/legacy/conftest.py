from functools import partial

from kombu import Connection
import pytest

from nameko.legacy.testing import reset_state

connections = []


def _get_connection(uri):
    conn = Connection(uri, transport_options={'confirm_publish': True})
    connections.append(conn)
    return conn


def close_connections():
    for c in connections:
        c.close()
    connections[:]


@pytest.yield_fixture
def connection(rabbit_config):
    amqp_uri = rabbit_config['AMQP_URI']

    yield _get_connection(amqp_uri)
    close_connections()


@pytest.yield_fixture
def get_connection(rabbit_config):
    amqp_uri = rabbit_config['AMQP_URI']

    yield partial(_get_connection, amqp_uri)
    close_connections()


@pytest.fixture(autouse=True)
def reset_mock_proxy(request):
    reset_state()
