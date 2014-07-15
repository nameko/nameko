from collections import deque

from kombu.common import eventloop
from kombu.messaging import Consumer


# lifted from kombu, modified to accept `ignore_timeouts`
def drain_consumer(consumer, limit=1, timeout=None, callbacks=None,
                   ignore_timeouts=True):

    acc = deque()

    def on_message(body, message):
        acc.append((body, message))

    consumer.callbacks = [on_message] + (callbacks or [])

    with consumer:
        client = consumer.channel.connection.client
        for _ in eventloop(client, limit=limit, timeout=timeout,
                           ignore_timeouts=ignore_timeouts):
            try:
                yield acc.popleft()
            except IndexError:
                pass


def queue_iterator(queue, no_ack=False, timeout=None):
    channel = queue.channel

    consumer = Consumer(channel, queues=[queue], no_ack=no_ack)
    for _, msg in drain_consumer(consumer, limit=None, timeout=timeout,
                                 ignore_timeouts=False):
        yield msg
