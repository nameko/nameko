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

        self.max_workers = poolsize
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

        self.workers = set()
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

        gt = self.procpool.spawn(self.handle_request, body, message)

        gt.link(self.handle_request_processed, message)
        self.workers.add(gt)

    def handle_request(self, body, message):
        # item is patched on for python with ``with``, pylint can't find it
        # pylint: disable=E1102
        with self._connection_pool.item() as connection:
            nameko.process_message(connection, self.controller, body)

    def handle_request_processed(self, gt, message):
        self.workers.discard(gt)
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

    def kill(self, force=False):
        _log.debug('killing service')

        if self._consumers:
            _log.debug('cancelling consumer')
            for consumer in self._consumers:
                consumer.cancel()

        if force:
            _log.debug('froce killing %d workers', len(self.workers))
            # need to list so that we don't get an exception
            # that the set changes as we iterate over it
            while self.workers:
                self.workers.pop().kill()
        else:
            pool = self.procpool
            _log.debug('waiting for %d workers to complete', pool.running())
            pool.waitall()

        # greenlet has a magic attribute ``dead`` - pylint: disable=E1101
        if self.greenlet is not None and not self.greenlet.dead:
            _log.debug('waiting for consumer to stop after message ack()')
            self._should_stop_after_ack = True
            self.greenlet.wait()

        if self._channel is not None:
            _log.debug('closing channel')
            self._channel.close()

    def link(self, *args, **kwargs):
        return self.greenlet.link(*args, **kwargs)
