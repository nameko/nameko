from eventlet.green.Queue import Queue
import eventlet

import pytest

from nameko.memory import MultiQueueConsumer


class FooError(Exception):
    pass


def test_multiqueue_wait():
    # purely a coverage test
    # Not sure what would cause thread waiting on a queue
    # to be killed, but it is handled and we want to have any kill exception
    # raised when waiting on the consumer.

    q = Queue()
    consumer = MultiQueueConsumer([q])

    eventlet.spawn(consumer.wait, 0.5)
    eventlet.sleep()

    for waiting_thread in q.getters:
        waiting_thread.kill(FooError('foobar'))

    with pytest.raises(FooError):
        consumer.wait(0.1)
