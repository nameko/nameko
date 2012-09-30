""" Modifications to the memory transport in kombu, used to help in testing.

Adds support for fanout exchanges and, with eventlet, faster messaging by
removing the queue polling.

installed with `memory.patch()`

"""

import eventlet
from eventlet.event import Event
from eventlet.green import Queue
from kombu.transport import memory as _memory


class MultiQueueConsumer(object):
    def __init__(self, queues):
        self.cancelled = False
        self.event = Event()
        self.queues = queues

    class Waiter(object):
        def __init__(self, consumer, queue):
            self.consumer = consumer
            self.queue = queue

        @property
        def cancelled(self):
            return self.consumer.cancelled

        def switch(self, item):
            if self.cancelled or self.consumer.event.ready():
                self.queue.queue.appendleft(item)
                self.queue._schedule_unlock()
            else:
                self.consumer.event.send((self.queue, item))

        def kill(self, *exc_info):
            if not self.cancelled and not self.consumer.event.ready():
                self.consumer.event.send(exc=exc_info)

    def wait(self, timeout=None, return_queue=False):
        empty_queues = []
        for q in self.queues:
            try:
                if return_queue:
                    return q, q.get_nowait()
                else:
                    return q.get_nowait()
            except Queue.Empty:
                empty_queues.append(q)
        for q in empty_queues:
            q.getters.add(self.Waiter(self, q))
        self.cancelled = False
        try:
            with eventlet.Timeout(timeout, exception=Queue.Empty):
                if return_queue:
                    return self.event.wait()
                else:
                    return self.event.wait()[1]
        finally:
            self.cancelled = True


class Channel(_memory.Channel):
    supports_fanout = True
    _fanout_queues = {}

    def _queue_bind(self, exchange, routing_key, pattern, queue):
        if self.typeof(exchange).type == "fanout":
            self._fanout_queues.setdefault(exchange, []).append(queue)

    def _put_fanout(self, exchange, message, **kwargs):
        for queue in self._fanout_queues.get(exchange, []):
            self._put(queue, message)

    def _get_many(self, queues, timeout=None):
        queues = [(self._queue_for(q), q) for q in queues]

        consumer = MultiQueueConsumer([q[0] for q in queues])
        queue, item = consumer.wait(timeout=timeout, return_queue=True)
        return item, dict(queues)[queue]


def patch():
    _memory.Transport.Channel = Channel
    _memory.Queue = Queue.Queue
