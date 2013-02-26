import eventlet
from eventlet.greenpool import GreenPool
from eventlet.event import Event
from eventlet.semaphore import Semaphore
import greenlet
from kombu.mixins import ConsumerMixin

import nameko
from nameko import entities
from nameko.common import UIDGEN


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
        self.consume_ready = Event()

        node_topic = "{}.{}".format(self.topic, self.nodeid)
        self.queues = [entities.get_topic_queue(exchange, topic),
                       entities.get_topic_queue(exchange, node_topic),
                       entities.get_fanout_queue(topic), ]
        self._channel = None
        self._consumers = None

    def start(self):
        if self.greenlet is not None and not self.greenlet.dead:
            raise RuntimeError()
        self.greenlet = eventlet.spawn(self.run)

    def get_consumers(self, Consumer, channel):
        return [Consumer(self.queues, callbacks=[self.on_message, ]), ]

    def on_consume_ready(self, connection, channel, consumers, **kwargs):
        self._consumers = consumers
        self._channel = channel
        self.consume_ready.send(None)

    def on_consume_end(self, connection, channel):
        self.consume_ready.reset()

    def on_message(self, body, message):
        # need a semaphore to stop killing between message ack()
        # and spawning process.
        with self.messagesem:
            self.procpool.spawn(self.handle_request, body)
            message.ack()

    def handle_request(self, body):
        nameko.process_message(self.connection, self.controller, body)

    def wait(self):
        try:
            self.greenlet.wait()
        except greenlet.GreenletExit:
            pass
        return self.procpool.waitall()

    def kill(self):
        if self.greenlet is not None and not self.greenlet.dead:
            self.should_stop = True
            #with self.messagesem:
                #self.greenlet.kill()
            self.greenlet.wait()
        if self._consumers:
            for c in self._consumers:
                c.cancel()
        if self._channel is not None:
            self._channel.close()

    def link(self, *args, **kwargs):
        return self.greenlet.link(*args, **kwargs)
