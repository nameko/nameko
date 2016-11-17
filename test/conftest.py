import json
import subprocess
import sys
import time
import uuid

import pytest
import requests
from mock import ANY, patch
from six.moves import queue
from six.moves.urllib.parse import urlparse
from types import ModuleType


TOXIPROXY_HOST = "127.0.0.1"
TOXIPROXY_PORT = 8474


@pytest.yield_fixture
def mock_producer():
    with patch('nameko.amqp.producers') as patched:
        producer = patched[ANY].acquire().__enter__()
        # normal behaviour is for no messages to be returned
        producer.channel.returned_messages.get_nowait.side_effect = queue.Empty
        yield producer


@pytest.yield_fixture
def mock_connection():
    with patch('nameko.amqp.connections') as patched:
        yield patched[ANY].acquire().__enter__()


@pytest.yield_fixture(scope='session')
def toxiproxy_server():
    # start a toxiproxy server
    host = TOXIPROXY_HOST
    port = TOXIPROXY_PORT
    server = subprocess.Popen(
        ['toxiproxy-server', '-port', str(port), '-host', host],
        stdout=subprocess.PIPE
    )
    time.sleep(0.2)  # allow server to start
    yield "{}:{}".format(host, port)
    server.terminate()


@pytest.yield_fixture
def toxiproxy(toxiproxy_server, rabbit_config, free_port):
    """ Insert a toxiproxy in front of RabbitMQ

    https://github.com/douglas/toxiproxy-python is not released yet, so
    we use requests to control the server.
    """

    # extract rabbit connection details
    amqp_uri = rabbit_config['AMQP_URI']
    uri = urlparse(amqp_uri)
    rabbit_port = uri.port

    proxy_port = free_port

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
    proxy_uri = amqp_uri.replace(str(rabbit_port), str(proxy_port))

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

        def timeout(self, timeout=500):
            resource = 'http://{}/proxies/{}/toxics'.format(
                toxiproxy_server, proxy_name
            )
            data = {
                'name': toxic_name,
                'type': 'timeout',
                'stream': 'upstream',
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

        def reset(self):
            # ensure the proxy passes traffic healthily again, so test cleanup
            # doesn't get stuck trying to reconnect
            self.enable()
            self.reset_timeout()

    controller = Controller(proxy_uri)
    yield controller
    controller.reset()


@pytest.yield_fixture
def fake_module():
    module = ModuleType("fake_module")
    sys.modules[module.__name__] = module
    yield module
    del sys.modules[module.__name__]
