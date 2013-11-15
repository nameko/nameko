import eventlet
from kombu.common import itermessages

from nameko.exceptions import WaiterTimeout


def queue_iterator(queue, no_ack=False, timeout=None):
    channel = queue.channel

    with eventlet.Timeout(timeout, exception=WaiterTimeout()):
        for _, msg in itermessages(channel.connection, channel, queue,
                                   limit=None, no_ack=no_ack):
            yield msg
