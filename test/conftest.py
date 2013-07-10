from functools import partial
import logging
from urlparse import urlparse

import eventlet
eventlet.monkey_patch()

from kombu import Connection
from pyrabbit.api import Client
import pytest

running_services = []


def _get_connection(uri):
    conn = Connection(uri)
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

    parser.addoption(
        "--amqp-uri", action="store",
        default='amqp://guest:guest@localhost:5672/nameko',
        help=("The AMQP-URI to connect to rabbit with."))

    parser.addoption(
        "--rabbit-ctl-uri", action="store",
        default='http://guest:guest@localhost:15672',
        help=("The URI for rabbit's management API."))


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


@pytest.fixture
def reset_rabbit(request):
    config = request.config

    rabbit_ctl_uri = urlparse(config.getoption('rabbit_ctl_uri'))
    host_port = '{0.hostname}:{0.port}'.format(rabbit_ctl_uri)

    rabbit = Client(
        host_port, rabbit_ctl_uri.username, rabbit_ctl_uri.password)

    amqp_uri = urlparse(config.getoption('amqp_uri'))
    vhost = amqp_uri.path[1:]

    def del_vhost():
        try:
            rabbit.delete_vhost(vhost)
        except:
            pass

    request.addfinalizer(del_vhost)

    del_vhost()

    rabbit.create_vhost(vhost)
    rabbit.set_vhost_permissions(vhost, amqp_uri.username, '.*', '.*', '.*')


@pytest.fixture
def get_connection(request, reset_rabbit):
    amqp_uri = request.config.getoption('amqp_uri')
    return partial(_get_connection, amqp_uri)


@pytest.fixture
def connection(request, reset_rabbit):
    amqp_uri = request.config.getoption('amqp_uri')
    return _get_connection(amqp_uri)


@pytest.fixture
def start_service(request, reset_rabbit, get_connection):

    def _start_service(cls, service_name):
        # making sure we import this as late as possible
        # to get correct coverage
        from nameko.service import Service

        srv = Service(cls, get_connection, 'rpc', service_name)
        running_services.append(srv)
        srv.start()
        srv.consume_ready.wait()
        eventlet.sleep()
        return srv.service

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
def kill_services(request, reset_rabbit):

    def _kill_services(name):
        for s in running_services:
            if s.topic == name:
                s.kill()

    return _kill_services
