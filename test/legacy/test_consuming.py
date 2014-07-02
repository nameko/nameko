import socket

from mock import patch, Mock
import pytest

from nameko.exceptions import WaiterTimeout
from nameko.legacy import consuming


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
        with pytest.raises(WaiterTimeout):
            list(consuming.queue_iterator(queue, timeout=0.1))
