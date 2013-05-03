from kombu.common import itermessages


def queue_iterator(queue, channel=None, no_ack=False, timeout=None):
    if queue.is_bound:
        if channel is not None:
            raise TypeError('channel specified when queue is bound')
        channel = queue.channel
    elif channel is not None:
        queue.bind(channel)
    else:
        raise TypeError('channel can not be None for unbound queue')
    channel = queue.channel

    for _, msg in itermessages(channel.connection, channel,
            queue, no_ack=no_ack, timeout=timeout):
        yield msg

