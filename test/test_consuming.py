import mock

from nameko.consuming import queue_iterator


def test_custom_channel_missing__message_to_python():
    # This is purely a code coverage test. see nameko.consuming.queue_iterator
    # comments regarding catching AttributeError.
    queue = mock.Mock()
    channel = queue.channel
    channel.message_to_python.side_effect = AttributeError('message_to_python')

    message = 'foobar'

    queue.consume.side_effect = lambda callback, no_ack: callback(message)

    with mock.patch('nameko.consuming.consumefrom', lambda conn: None):

        item = iter(queue_iterator(queue)).next()
        assert item == message
