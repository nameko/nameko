""" Barebones conftest.py
"""
# Nameko relies on eventlet
# You should monkey patch the standard library as early as possible to avoid
# importing anything before the patch is applied.
# See http://eventlet.net/doc/patching.html#monkeypatching-the-standard-library
import eventlet
eventlet.monkey_patch()

from urlparse import urlparse

from pyrabbit.api import Client
import pytest


def pytest_addoption(parser):
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


@pytest.fixture(autouse=True)
def reset_kombu_pools(request):
    from kombu.pools import reset
    reset()


@pytest.fixture(scope='session')
def rabbit_manager(request):
    config = request.config

    rabbit_ctl_uri = urlparse(config.getoption('RABBIT_CTL_URI'))
    host_port = '{0.hostname}:{0.port}'.format(rabbit_ctl_uri)

    rabbit = Client(
        host_port, rabbit_ctl_uri.username, rabbit_ctl_uri.password)

    return rabbit


@pytest.yield_fixture
def rabbit_config(request, rabbit_manager):
    amqp_uri = request.config.getoption('AMQP_URI')

    conf = {'AMQP_URI': amqp_uri}

    uri = urlparse(amqp_uri)
    vhost = uri.path[1:].replace('/', '%2F')
    username = uri.username

    conf['vhost'] = vhost
    conf['username'] = username

    def del_vhost():
        try:
            rabbit_manager.delete_vhost(vhost)
        except:
            pass

    del_vhost()
    rabbit_manager.create_vhost(vhost)
    rabbit_manager.set_vhost_permissions(vhost, username, '.*', '.*', '.*')

    yield conf

    del_vhost()
