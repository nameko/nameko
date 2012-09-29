import eventlet
from eventlet.greenpool import GreenPool
from eventlet.semaphore import Semaphore
import greenlet
from kombu.mixins import ConsumerMixin

import newrpc
from newrpc import entities
from newrpc.common import UIDGEN


class Service(ConsumerMixin):
    def __init__(self, controllercls,
            connection, exchange, topic,
            pool=None, poolsize=1000):
        self.nodeid = UIDGEN()

        if pool is None:
            self.procpool = GreenPool(size=poolsize)
        else:
            self.procpool = pool

        self.connection = connection
        self.controller = controllercls()
        self.topic = topic
        self.greenlet = None
        self.messagesem = Semaphore()

        node_topic = "{}.{}".format(self.topic, self.nodeid)
        self.queues = [entities.get_topic_queue(exchange, topic),
                       entities.get_topic_queue(exchange, node_topic),
                       entities.get_fanout_queue(topic), ]

    def start(self):
        # self.connection = newrpc.create_connection()
        if self.greenlet is not None and not self.greenlet.dead:
            raise RuntimeError()
        self.greenlet = eventlet.spawn(self.run)

    def get_consumers(self, Consumer, channel):
        return [Consumer(self.queues, callbacks=[self.on_message, ]), ]

    def on_message(self, body, message):
        # need a semaphore to stop killing between message ack()
        # and spawning process.
        with self.messagesem:
            self.procpool.spawn(self.handle_request, body)
            message.ack()

    def handle_request(self, body):
        newrpc.delegate_applyreply(self.connection, self.controller, body)

    def wait(self):
        try:
            self.greenlet.wait()
        except greenlet.GreenletExit:
            pass
        return self.procpool.waitall()

    def kill(self):
        if self.greenlet is not None and not self.greenlet.dead:
            with self.messagesem:
                self.greenlet.kill()
        for c in self.connection.consumers:
            c.cancel()

    def link(self, *args, **kwargs):
        return self.greenlet.link(*args, **kwargs)

    def kill_processes(self):
        for g in self.procpool.coroutines_running:
            g.kill()
