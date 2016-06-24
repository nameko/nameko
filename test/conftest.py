import pytest
from mock import ANY, patch
from six.moves import queue


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
