import eventlet
eventlet.monkey_patch()

from kombu import BrokerConnection

from newrpc import memory
memory.patch()


def get_connection():
    conn = BrokerConnection(transport='memory')
    return conn


def pytest_funcarg__get_connection(request):
    return get_connection


def pytest_funcarg__connection(request):
    return get_connection()


def pytest_runtest_teardown(item, nextitem):
    memory._memory.Transport.state.clear()
