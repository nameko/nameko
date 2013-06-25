from mock import patch, Mock
import pytest

from nameko import consuming, exceptions


@patch('nameko.consuming.itermessages')
class TestQueueIteratorTimeout(object):

    def test_no_timeout(self, itermessages):
        itermessages.return_value = [(1, 'foo'), (2, 'bar')]
        queue = Mock()
        res = list(consuming.queue_iterator(queue, timeout=1))
        assert res == ['foo', 'bar']

    def test_timeout(self, itermessages):
        import time
        itermessages.side_effect = lambda *a, **kw: time.sleep(1)
        queue = Mock()
        with pytest.raises(exceptions.WaiterTimeout):
            list(consuming.queue_iterator(queue, timeout=0.1))
