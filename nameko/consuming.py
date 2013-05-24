from kombu.common import itermessages


def queue_iterator(queue, no_ack=False, timeout=None):
    channel = queue.channel

    for _, msg in itermessages(channel.connection, channel,
            queue, limit=None, no_ack=no_ack, timeout=timeout):
        yield msg
