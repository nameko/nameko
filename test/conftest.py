import json
import socket
import subprocess
import time
import uuid

import pytest
import requests
from mock import ANY, patch
from six.moves import queue
from six.moves.urllib.parse import urlparse


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


@pytest.fixture
def toxiproxy_host():
    return "127.0.0.1"


@pytest.fixture
def toxiproxy_port(toxiproxy_host):
    return 8474


@pytest.yield_fixture
def toxiproxy_server(toxiproxy_host, toxiproxy_port):
    # start a toxiproxy server
    host = toxiproxy_host
    port = toxiproxy_port
    server = subprocess.Popen(
        ['toxiproxy-server', '-port', str(port), '-host', host],
        stdout=subprocess.PIPE
    )
    time.sleep(0.2)  # allow server to start
    yield "{}:{}".format(host, port)
    server.terminate()


@pytest.fixture
def toxiproxy(toxiproxy_server, rabbit_config):
    """ Insert a toxiproxy in front of RabbitMQ

    https://github.com/douglas/toxiproxy-python is not released yet, so
    we use requests to control the server.
    """

    # extract rabbit connection details
    amqp_uri = rabbit_config['AMQP_URI']
    uri = urlparse(amqp_uri)
    rabbit_port = uri.port

    # find a free port
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((uri.hostname, 0))
    proxy_port = sock.getsockname()[1]
    sock.close()

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

    return Controller(proxy_uri)
