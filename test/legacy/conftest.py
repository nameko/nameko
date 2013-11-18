from functools import partial

from kombu import Connection
import pytest

from nameko.legacy.testing import reset_state

connections = []


def _get_connection(uri):
    conn = Connection(uri)
    connections.append(conn)
    return conn


def close_connections():
    for c in connections:
        c.close()
    connections[:]


@pytest.fixture
def connection(request, reset_rabbit):
    amqp_uri = request.config.getoption('AMQP_URI')

    request.addfinalizer(close_connections)
    return _get_connection(amqp_uri)


@pytest.fixture
def get_connection(request, reset_rabbit):
    amqp_uri = request.config.getoption('AMQP_URI')

    request.addfinalizer(close_connections)
    return partial(_get_connection, amqp_uri)


@pytest.fixture(autouse=True)
def reset_mock_proxy(request):
    reset_state()
