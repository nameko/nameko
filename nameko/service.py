from __future__ import absolute_import
from logging import getLogger

import eventlet
from eventlet.pools import Pool
from eventlet.greenpool import GreenPool
from eventlet.event import Event
from kombu.mixins import ConsumerMixin

import nameko
from nameko import entities
from nameko.common import UIDGEN

_log = getLogger(__name__)


class Service(ConsumerMixin):
    def __init__(
            self, controllercls, connection_factory, exchange, topic,
            pool=None, poolsize=1000):
        self.nodeid = UIDGEN()

        if pool is None:
            self.procpool = GreenPool(size=poolsize)
        else:
            self.procpool = pool

        self.controller = controllercls()
        self.topic = topic
        self.greenlet = None
        self.consume_ready = Event()

        node_topic = "{}.{}".format(self.topic, self.nodeid)
        self.queues = [entities.get_topic_queue(exchange, topic),
                       entities.get_topic_queue(exchange, node_topic),
                       entities.get_fanout_queue(topic), ]
        self._channel = None
        self._consumers = None

        self.connection = connection_factory()
        self._connection_pool = Pool(
            max_size=self.procpool.size,
            create=connection_factory
        )

        self._pending_messages = []
        self._should_stop_after_ack = False

    def start(self):
        # greenlet has a magic attribute ``dead`` - pylint: disable=E1101
        if self.greenlet is not None and not self.greenlet.dead:
            raise RuntimeError()
        self.greenlet = eventlet.spawn(self.run)

    def get_consumers(self, Consumer, channel):
        consumer = Consumer(self.queues, callbacks=[self.on_message, ])
        consumer.qos(prefetch_count=self.procpool.size)
        return [consumer, ]

    def on_consume_ready(self, connection, channel, consumers, **kwargs):
        self._consumers = consumers
        self._channel = channel
        self.consume_ready.send(None)

    def on_consume_end(self, connection, channel):
        self.consume_ready.reset()

    def on_message(self, body, message):
        _log.debug('spawning worker (%d free)', self.procpool.free())
        self.procpool.spawn(self.handle_request, body, message)

    def handle_request(self, body, message):
        try:
            # item is patched on for python with ``with``, pylint can't find it
            # pylint: disable=E1102
            with self._connection_pool.item() as connection:
                nameko.process_message(connection, self.controller, body)
        finally:
            self._pending_messages.append(message)

    def on_iteration(self):
        self.ack_pending_messages()

    def ack_pending_messages(self):
        messages = self._pending_messages

        if messages:
            _log.debug('ack() %d processed messages', len(messages))
            while messages:
                msg = messages.pop()
                msg.ack()
                eventlet.sleep()

        if self._should_stop_after_ack:
            _log.debug('notifying consumer to stop')
            self.should_stop = True

    def kill(self):
        _log.debug('killing service, disabling consumption of messages')
        if self._consumers is not None:
            # flow() does not work on virtual transports, so we just use QoS
            self._consumers[0].qos(prefetch_count=0)

        _log.debug('waiting for workers to complete')
        self.procpool.waitall()

        # greenlet has a magic attribute ``dead`` - pylint: disable=E1101
        if self.greenlet is not None and not self.greenlet.dead:
            _log.debug('waiting for consumer to stop after message ack()')
            self._should_stop_after_ack = True
            self.greenlet.wait()

        if self._consumers:
            _log.debug('cancelling consumer')
            for c in self._consumers:
                c.cancel()

        if self._channel is not None:
            _log.debug('cancelling channel')
            self._channel.close()

    def link(self, *args, **kwargs):
        return self.greenlet.link(*args, **kwargs)
