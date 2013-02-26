import eventlet
eventlet.monkey_patch()

from kombu import BrokerConnection

from nameko import memory
memory.patch()


def get_connection():
    conn = BrokerConnection(transport='memory')
    return conn


def pytest_addoption(parser):
    parser.addoption('--blocking-detection',
        action='store_true',
        dest='blocking_detection',
        help='turn on eventlet hub blocking detection')


def pytest_configure(config):
    if config.option.blocking_detection:
        from eventlet import debug
        debug.hub_blocking_detection(True)


def pytest_funcarg__get_connection(request):
    return get_connection


def pytest_funcarg__connection(request):
    return get_connection()


def pytest_runtest_teardown(item, nextitem):
    memory._memory.Transport.state.clear()
