import logging

import eventlet
eventlet.monkey_patch()

from kombu import Connection
from pyrabbit.api import Client
import pytest

running_services = []


def _get_connection():
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


def _start_service(cls, service_name):
    # making sure we import this as late as possible to get correct coverage
    from nameko.service import Service

    srv = Service(cls, _get_connection, 'rpc', service_name)
    running_services.append(srv)
    srv.start()
    srv.consume_ready.wait()
    eventlet.sleep()
    return srv.service


def _kill_service(name):
    for s in running_services:
        if s.topic == name:
            s.kill()


@pytest.fixture
def reset_rabbit(request):
    rabbit = Client('localhost:15672', 'guest', 'guest')

    def del_vhost():
        try:
            rabbit.delete_vhost('nameko')
        except:
            pass

    request.addfinalizer(del_vhost)

    del_vhost()
    rabbit.create_vhost('nameko')
    rabbit.set_vhost_permissions('nameko', 'guest', '.*', '.*', '.*')


@pytest.fixture
def get_connection(request, reset_rabbit):
    return _get_connection


@pytest.fixture
def connection(request, reset_rabbit):
    return _get_connection()


@pytest.fixture
def start_service(request, reset_rabbit):

    def kill_services():
        for s in running_services:
            try:
                s.kill()
                # TODO: need to delete all queues
            except:
                pass
        del running_services[:]

    request.addfinalizer(kill_services)
    return _start_service


@pytest.fixture
def kill_service(request, reset_rabbit):
    return _kill_service
