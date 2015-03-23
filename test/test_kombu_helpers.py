import socket

from eventlet.event import Event
from mock import patch, Mock, MagicMock
import pytest

from nameko.exceptions import RpcTimeout
from nameko.kombu_helpers import queue_iterator, drain_consumer
from nameko.rpc import rpc
from nameko.standalone.rpc import ServiceRpcProxy


class TestQueueIteratorTimeout(object):

    @patch('nameko.kombu_helpers.drain_consumer', autospec=True)
    def test_no_timeout(self, drain_consumer):
        drain_consumer.return_value = [(1, 'foo'), (2, 'bar')]
        queue = Mock()
        res = list(queue_iterator(queue, timeout=1))
        assert res == [(1, 'foo'), (2, 'bar')]

    @patch('nameko.kombu_helpers.eventloop', autospec=True)
    def test_timeout_raises_rpc_exc_on_timeout(self, eventloop):

        eventloop.side_effect = socket.timeout

        queue = Mock()
        with pytest.raises(RpcTimeout):
            list(queue_iterator(queue, timeout=0.1))

    @patch('nameko.kombu_helpers.eventloop', autospec=True)
    def test_timeout_reraises_if_no_timeout(self, eventloop):

        eventloop.side_effect = socket.timeout

        queue = Mock()
        with pytest.raises(socket.timeout):
            list(queue_iterator(queue, timeout=None))

    def test_end_to_end(self, container_factory, rabbit_config):

        class Service(object):
            name = "service"

            @rpc
            def wait(self):
                Event().wait()

        container = container_factory(Service, rabbit_config)
        container.start()

        with ServiceRpcProxy("service", rabbit_config, timeout=.01) as proxy:
            with pytest.raises(RpcTimeout):
                proxy.wait()

        # container won't stop gracefully with a running worker
        container.kill()

    @patch('nameko.kombu_helpers.eventloop', autospec=True)
    def test_drain_consumer(self, eventloop):

        def receive_message(client, **kwargs):
            client.consumer.callbacks[0](1, "foo")
            return [True]

        eventloop.side_effect = receive_message

        consumer = MagicMock()
        client = Mock()

        # nasty circular reference so we can access the callback
        client.consumer = consumer
        consumer.channel.connection.client = client

        assert list(drain_consumer(consumer)) == [(1, 'foo')]

    @patch('nameko.kombu_helpers.eventloop', autospec=True)
    def test_drain_consumer_no_messages(self, eventloop):

        eventloop.return_value = [True]  # no message arrives

        consumer = MagicMock()
        assert list(drain_consumer(consumer)) == []
