import logging

import eventlet
eventlet.monkey_patch()

from kombu import BrokerConnection

from nameko import memory
memory.patch()


def get_connection():
    conn = BrokerConnection(transport='memory')
    return conn


def pytest_addoption(parser):
    parser.addoption(
        '--blocking-detection',
        action='store_true',
        dest='blocking_detection',
        help='turn on eventlet hub blocking detection')

    parser.addoption(
        "--log-level", action="store",
        default=None,
        help=("The logging-level for the test run."))


def pytest_configure(config):
    if config.option.blocking_detection:
        from eventlet import debug
        debug.hub_blocking_detection(True)

    log_level = config.getoption('log_level')
    if log_level is not None:
        logging.basicConfig(level=getattr(logging, log_level))
        logging.getLogger('py2neo').setLevel(logging.ERROR)


def pytest_funcarg__get_connection(request):
    return get_connection


def pytest_funcarg__connection(request):
    return get_connection()


def pytest_runtest_teardown(item, nextitem):
    memory._memory.Transport.state.clear()
