import logging

import eventlet
eventlet.monkey_patch()


logging.basicConfig(level=logging.DEBUG)

from kombu import Connection


def get_connection():
    #conn = Connection('amqp://guest:guest@10.11.105.128:5672//platform')
    conn = Connection(transport='memory')

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
    # monkey patch an encoding attribute onto GreenPipe to
    # satisfy a pytest assertion
    import py
    from eventlet.greenio import GreenPipe
    GreenPipe.encoding = py.std.sys.stdout.encoding

    if config.option.blocking_detection:
        from eventlet import debug
        debug.hub_blocking_detection(True)

    log_level = config.getoption('log_level')
    if log_level is not None:
        logging.basicConfig(level=getattr(logging, log_level))


def pytest_funcarg__get_connection(request):
    return get_connection


def pytest_funcarg__connection(request):
    return get_connection()


def pytest_runtest_setup(item):
    # we cannot patch it on a module level,
    # as it would skew coverage reports
    from nameko import memory
    memory.patch()


def pytest_runtest_teardown(item, nextitem):
    from nameko import memory
    memory._memory.Transport.state.clear()
