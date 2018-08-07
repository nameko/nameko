import json
import subprocess
import sys
import uuid
from contextlib import contextmanager
from types import ModuleType

import eventlet
import pytest
import requests
from mock import ANY, patch
from six.moves import queue
from six.moves.urllib.parse import urlparse

from nameko.testing.utils import find_free_port
from nameko.utils.retry import retry


TOXIPROXY_HOST = "127.0.0.1"
TOXIPROXY_PORT = 8474


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "publish_retry: distinguish tests that should use retry in publishers"
    )
    config.addinivalue_line(
        "markers",
        "behavioural: distinguish behavioural tests"
    )


@pytest.yield_fixture
def mock_producer():
    with patch('nameko.amqp.publish.producers') as patched:
        with patched[ANY].acquire() as producer:
            # normal behaviour is for no messages to be returned
            producer.channel.returned_messages.get_nowait.side_effect = (
                queue.Empty
            )
            yield producer


@pytest.yield_fixture
def mock_channel():
    with patch('nameko.amqp.publish.connections') as patched:
        with patched[ANY].acquire() as connection:
            yield connection.channel()


@pytest.yield_fixture(scope='session')
def toxiproxy_server():
    # start a toxiproxy server
    host = TOXIPROXY_HOST
    port = TOXIPROXY_PORT
    server = subprocess.Popen(
        ['toxiproxy-server', '-port', str(port), '-host', host]
    )

    class NotReady(Exception):
        pass

    @retry(delay=0.1, max_attempts=10)
    def wait_until_server_ready():
        url = "http://{}:{}/proxies".format(TOXIPROXY_HOST, TOXIPROXY_PORT)
        res = requests.get(url)
        if not res.status_code == 200:  # pragma: no cover
            raise NotReady("toxiproxy-server failed to start")

    wait_until_server_ready()
    yield "{}:{}".format(host, port)
    server.terminate()


@pytest.yield_fixture
def toxiproxy(toxiproxy_server, rabbit_config):
    """ Insert a toxiproxy in front of RabbitMQ

    https://github.com/douglas/toxiproxy-python is not released yet, so
    we use requests to control the server.
    """

    # extract rabbit connection details
    amqp_uri = rabbit_config['AMQP_URI']
    uri = urlparse(amqp_uri)
    rabbit_port = uri.port

    proxy_port = find_free_port()

    # create proxy
    proxy_name = "nameko_test_rabbitmq_{}".format(uuid.uuid4().hex)

    listen = "{}:{}".format(uri.hostname, proxy_port)
    upstream = "{}:{}".format(uri.hostname, rabbit_port)
    requests.post(
        'http://{}/proxies'.format(toxiproxy_server),
        data=json.dumps({
            'name': proxy_name,
            'listen': listen,
            'upstream': upstream
        })
    )

    # create proxied uri for publisher
    proxy_uri = "{}://{}:{}@{}{}".format(
        uri.scheme, uri.username, uri.password, listen, uri.path
    )

    toxic_name = '{}_timeout'.format(proxy_name)

    class Controller(object):

        def __init__(self, proxy_uri):
            self.uri = proxy_uri

        def enable(self):
            resource = 'http://{}/proxies/{}'.format(
                toxiproxy_server, proxy_name
            )
            data = {
                'enabled': True
            }
            requests.post(resource, json.dumps(data))

        def disable(self):
            resource = 'http://{}/proxies/{}'.format(
                toxiproxy_server, proxy_name
            )
            data = {
                'enabled': False
            }
            requests.post(resource, json.dumps(data))

        def set_timeout(self, timeout=500, stream="upstream"):
            resource = 'http://{}/proxies/{}/toxics'.format(
                toxiproxy_server, proxy_name
            )
            data = {
                'name': toxic_name,
                'type': 'timeout',
                'stream': stream,
                'attributes': {
                    'timeout': timeout
                }
            }
            requests.post(resource, json.dumps(data))

        def reset_timeout(self):
            resource = 'http://{}/proxies/{}/toxics/{}'.format(
                toxiproxy_server, proxy_name, toxic_name
            )
            requests.delete(resource)

        @contextmanager
        def disabled(self):
            self.disable()
            yield
            self.enable()

        @contextmanager
        def timeout(self, timeout=500, stream="upstream"):
            self.set_timeout(timeout=timeout, stream=stream)
            yield
            self.reset_timeout()

    controller = Controller(proxy_uri)
    yield controller

    # delete proxy
    # allow some grace period to ensure we don't remove the proxy before
    # other fixtures have torn down
    resource = 'http://{}/proxies/{}'.format(toxiproxy_server, proxy_name)
    eventlet.spawn_after(10, requests.delete, resource)


@pytest.yield_fixture
def fake_module():
    module = ModuleType("fake_module")
    sys.modules[module.__name__] = module
    yield module
    del sys.modules[module.__name__]
