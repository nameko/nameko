import eventlet
import itertools
from mock import patch

eventlet.monkey_patch()

import logging
import sys
from urlparse import urlparse

import pytest

from nameko.containers import ServiceContainer, WorkerContext
from nameko.runners import ServiceRunner
from nameko.testing.utils import (
    get_rabbit_manager, reset_rabbit_vhost, reset_rabbit_connections)


def pytest_addoption(parser):
    parser.addoption(
        '--blocking-detection',
        action='store_true',
        dest='blocking_detection',
        default=False,
        help='turn on eventlet hub blocking detection')

    parser.addoption(
        "--log-level", action="store",
        default='DEBUG',
        help=("The logging-level for the test run."))

    parser.addoption(
        "--amqp-uri", action="store", dest='AMQP_URI',
        default='amqp://guest:guest@localhost:5672/nameko',
        help=("The AMQP-URI to connect to rabbit with."))

    parser.addoption(
        "--rabbit-ctl-uri", action="store", dest='RABBIT_CTL_URI',
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
        log_level = getattr(logging, log_level)
        logging.basicConfig(level=log_level, stream=sys.stderr)


@pytest.fixture(autouse=True)
def reset_kombu_pools(request):
    from kombu.pools import reset
    reset()


@pytest.fixture
def empty_config(request):
    return {}


@pytest.fixture(scope='session')
def rabbit_manager(request):
    config = request.config
    return get_rabbit_manager(config.getoption('RABBIT_CTL_URI'))


@pytest.yield_fixture()
def rabbit_config(request, rabbit_manager):
    amqp_uri = request.config.getoption('AMQP_URI')

    conf = {'AMQP_URI': amqp_uri}

    uri = urlparse(amqp_uri)
    vhost = uri.path[1:].replace('/', '%2F')
    username = uri.username

    conf['vhost'] = vhost
    conf['username'] = username

    reset_rabbit_vhost(vhost, username, rabbit_manager)
    reset_rabbit_connections(vhost, rabbit_manager)

    yield conf


@pytest.yield_fixture
def container_factory(rabbit_config):

    all_containers = []

    def make_container(service_cls, config, worker_ctx_cls=None):
        if worker_ctx_cls is None:
            worker_ctx_cls = WorkerContext

        container = ServiceContainer(service_cls, worker_ctx_cls, config)
        all_containers.append(container)
        return container

    yield make_container

    for c in all_containers:
        try:
            c.stop()
        except:
            pass


@pytest.yield_fixture
def runner_factory(rabbit_config):

    all_runners = []

    def make_runner(config, *service_classes):
        runner = ServiceRunner(config)
        for service_cls in service_classes:
            runner.add_service(service_cls)
        all_runners.append(runner)
        return runner

    yield make_runner

    for r in all_runners:
        try:
            r.stop()
        except:
            pass


@pytest.yield_fixture
def predictable_call_ids(request):
    with patch('nameko.containers.new_call_id', autospec=True) as get_id:
        get_id.side_effect = (str(i) for i in itertools.count())
        yield get_id
