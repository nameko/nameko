import logging

import eventlet
eventlet.monkey_patch()

from kombu import Connection

running_services = []


def get_connection():
    conn = Connection('amqp://guest:guest@localhost:5672/nameko')
    return conn


def pytest_addoption(parser):
    parser.addoption(
        '--blocking-detection',
        action='store_true',
        dest='blocking_detection',
        help='turn on eventlet hub blocking detection')

    parser.addoption(
        "--log-level", action="store",
        default='DEBUG',
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


def start_service(cls, service_name):
    # making sure we import this as late as possible to get correct coverage
    from nameko.service import Service

    srv = Service(cls, get_connection, 'rpc', service_name)
    running_services.append(srv)
    srv.start()
    srv.consume_ready.wait()
    eventlet.sleep()
    return srv.service


def kill_services():
    queues = set()

    for s in running_services:
        try:
            for cons in s._consumers:
                queues |= set(cons.queues)
            s.kill()
            # TODO: need to delete all queues
        except:
            pass
    del running_services[:]

    # TODO: this should really just delete all queues and exchanges on the
    # test-vhost
    with get_connection() as conn:
        for q in queues:
            q.exchange.bind(conn)
            try:
                q.exchange.delete()
            except:
                pass

            q.bind(conn)
            try:
                q.delete()
            except:
                pass


def kill_service(name):
    for idx, s in enumerate(running_services):
        if s.topic == name:
            s.kill()


def pytest_funcarg__get_connection(request):
    return get_connection


def pytest_funcarg__connection(request):
    return get_connection()


def pytest_funcarg__start_service(request):
    return start_service


def pytest_funcarg__kill_service(request):
    return kill_service


def pytest_runtest_setup(item):
    # we cannot patch it on a module level,
    # as it would skew coverage reports
    from nameko import memory
    memory.patch()


def pytest_runtest_teardown(item, nextitem):
    from nameko import memory
    memory._memory.Transport.state.clear()
    kill_services()
