from collections import deque
import socket

from kombu.common import eventloop
from kombu.messaging import Consumer

from nameko.exceptions import RpcTimeout


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
    try:
        for body, msg in drain_consumer(
            consumer, limit=None, timeout=timeout, ignore_timeouts=False
        ):
            yield body, msg
    except socket.timeout:
        if timeout is not None:
            # we raise a different exception type here because we bubble out
            # to our caller, but `socket.timeout` errors get caught if
            # our connection is "ensured" with `kombu.Connection.ensure`;
            # the reference to the connection is destroyed so it can't be
            # closed later - see https://github.com/celery/kombu/blob/v3.0.4/
            # kombu/connection.py#L446
            raise RpcTimeout(timeout)
        raise
