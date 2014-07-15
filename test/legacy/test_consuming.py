import socket

from eventlet.event import Event
from mock import patch, Mock, MagicMock
import pytest

from nameko.legacy import consuming
from nameko.legacy.dependencies import rpc
from nameko.legacy.proxy import RPCProxy


class TestQueueIteratorTimeout(object):

    @patch('nameko.legacy.consuming.drain_consumer', autospec=True)
    def test_no_timeout(self, drain_consumer):
        drain_consumer.return_value = [(1, 'foo'), (2, 'bar')]
        queue = Mock()
        res = list(consuming.queue_iterator(queue, timeout=1))
        assert res == ['foo', 'bar']

    @patch('nameko.legacy.consuming.eventloop', autospec=True)
    def test_timeout_raises(self, eventloop):

        eventloop.side_effect = socket.timeout

        queue = Mock()
        with pytest.raises(socket.timeout):
            list(consuming.queue_iterator(queue, timeout=0.1))

    def test_timeout(self, container_factory, rabbit_config):

        class NovaService(object):

            @rpc
            def wait(self):
                Event().wait()

        container = container_factory(NovaService, rabbit_config)
        container.start()

        uri = rabbit_config['AMQP_URI']
        proxy = RPCProxy(uri)

        with pytest.raises(socket.timeout):
            proxy.novaservice.wait(timeout=1)

        # container won't stop gracefully with a running worker
        container.kill()

    @patch('nameko.legacy.consuming.eventloop', autospec=True)
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

        assert list(consuming.drain_consumer(consumer)) == [(1, 'foo')]

    @patch('nameko.legacy.consuming.eventloop', autospec=True)
    def test_drain_consumer_no_messages(self, eventloop):

        eventloop.return_value = [True]  # no message arrives

        consumer = MagicMock()
        assert list(consuming.drain_consumer(consumer)) == []
