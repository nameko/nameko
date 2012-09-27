from eventlet.green import Queue
from kombu.transport import memory as _memory


class Channel(_memory.Channel):
    supports_fanout = True
    _fanout_queues = {}

    def _queue_bind(self, exchange, routing_key, pattern, queue):
        if self.typeof(exchange).type == "fanout":
            self._fanout_queues.setdefault(exchange, []).append(queue)

    def _put_fanout(self, exchange, message, **kwargs):
        for queue in self._fanout_queues.get(exchange, []):
            self._put(queue, message)


def patch():
    _memory.Transport.Channel = Channel
    _memory.Queue = Queue.Queue
