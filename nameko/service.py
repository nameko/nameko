from __future__ import absolute_import
from itertools import count
from logging import getLogger
import socket

import eventlet
from eventlet.pools import Pool
from eventlet.greenpool import GreenPool
from eventlet.event import Event
from kombu.mixins import ConsumerMixin

from nameko import entities
from nameko.common import UIDGEN
from nameko.messaging import get_consumers, process_message
from nameko.dependencies import inject_dependencies
from nameko.sending import process_rpc_message

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
        self.service = self.controller
        self.topic = topic
        self.greenlet = None
        self.consume_ready = Event()

        node_topic = "{}.{}".format(self.topic, self.nodeid)
        self.nova_queues = [
            entities.get_topic_queue(exchange, topic),
            entities.get_topic_queue(exchange, node_topic),
            entities.get_fanout_queue(topic), ]

        self._channel = None
        self._consumers = None

        self.connection = connection_factory()

        inject_dependencies(self.controller, self.connection)

        self._connection_pool = Pool(
            max_size=self.procpool.size,
            create=connection_factory
        )

        self.workers = set()
        self._pending_messages = []
        self._do_cancel_consumers = False
        self._consumers_cancelled = Event()

    def start(self):
        # greenlet has a magic attribute ``dead`` - pylint: disable=E1101
        if self.greenlet is not None and not self.greenlet.dead:
            raise RuntimeError()
        self.greenlet = eventlet.spawn(self.run)

    def get_consumers(self, Consumer, channel):
        nova_consumer = Consumer(
            self.nova_queues, callbacks=[self.on_nova_message, ])

        nova_consumer.qos(prefetch_count=self.procpool.size)

        consume_consumers = get_consumers(
            Consumer, self.controller, self.on_consume_message)

        return [nova_consumer] + list(consume_consumers)

    def on_consume_ready(self, connection, channel, consumers, **kwargs):
        self._consumers = consumers
        self._channel = channel
        self.consume_ready.send(None)

    def on_consume_end(self, connection, channel):
        self.consume_ready.reset()

    def on_nova_message(self, body, message):
        _log.debug('spawning worker (%d free)', self.procpool.free())

        gt = self.procpool.spawn(self.handle_request, body, message)

        gt.link(self.handle_request_processed, message)
        self.workers.add(gt)

    def on_consume_message(
            self, consumer_config, consumer_method, body, message):
        _log.debug('spawning consumer')

        self.procpool.spawn(
            process_message, consumer_config, consumer_method, body, message)

    def handle_request(self, body, message):
        # item is patched on for python with ``with``, pylint can't find it
        # pylint: disable=E1102
        with self._connection_pool.item() as connection:
            process_rpc_message(connection, self.controller, body)

    def handle_request_processed(self, gt, message):
        self.workers.discard(gt)
        self._pending_messages.append(message)

    def on_iteration(self):
        self.process_consumer_cancelation()
        # we need to make sure we process any pending messages before shutdown
        self.process_pending_message_ack()
        self.process_shutdown()

    def process_consumer_cancelation(self):
        if self._do_cancel_consumers:
            self._do_cancel_consumers = False
            if self._consumers:
                _log.debug('cancelling consumers')
                for consumer in self._consumers:
                    consumer.cancel()
            self._consumers_cancelled.send(True)

    def process_pending_message_ack(self):
        messages = self._pending_messages
        if messages:
            _log.debug('ack() %d processed messages', len(messages))
            while messages:
                msg = messages.pop()
                msg.ack()
                eventlet.sleep()

    def consume(self, limit=None, timeout=None, safety_interval=0.1, **kwargs):
        """ Lifted from kombu so we are able to break the loop immediately
            after a shutdown is triggered rather than waiting for the timeout.
        """
        elapsed = 0
        with self.Consumer() as (connection, channel, consumers):
            with self.extra_context(connection, channel):
                self.on_consume_ready(connection, channel, consumers, **kwargs)
                for i in limit and xrange(limit) or count():
                    # moved from after the following `should_stop` condition to
                    # avoid waiting on a drain_events timeout before breaking
                    # the loop.
                    self.on_iteration()
                    if self.should_stop:
                        break

                    try:
                        connection.drain_events(timeout=safety_interval)
                    except socket.timeout:
                        elapsed += safety_interval
                        if timeout and elapsed >= timeout:
                            raise socket.timeout()
                    except socket.error:
                        if not self.should_stop:
                            raise
                    else:
                        yield
                        elapsed = 0

    def process_shutdown(self):
        if (
            self._consumers_cancelled.ready() and
            self.procpool.running() < 1 and
            not self._pending_messages
        ):
            _log.debug('notifying service to stop')
            self.should_stop = True

    def cancel_consumers(self):
        # greenlet has a magic attribute ``dead`` - pylint: disable=E1101
        if self.greenlet is not None and not self.greenlet.dead:
            # since consumers were started in a separate thread,
            # we will just notify the thread to avoid getting
            # "Second simultaneous read" errors
            _log.debug('notifying consumers to be cancelled')
            self._do_cancel_consumers = True
            self._consumers_cancelled.wait()
        else:
            _log.debug('consumer thread already dead')

    def kill_workers(self):
        _log.debug('force killing %d workers', len(self.workers))
        while self.workers:
            self.workers.pop().kill()

    def wait_for_workers(self):
        pool = self.procpool
        _log.debug('waiting for %d workers to complete', pool.running())
        pool.waitall()

    def shut_down(self):
        # greenlet has a magic attribute ``dead`` - pylint: disable=E1101
        if self.greenlet is not None and not self.greenlet.dead:
            _log.debug('stopping service')
            self.greenlet.wait()

        # TODO: when is this ever not None?
        if self._channel is not None:
            _log.debug('closing channel')
            self._channel.close()

    def kill(self, force=False):
        _log.debug('killing service')

        self.cancel_consumers()

        if force:
            self.kill_workers()
        else:
            self.wait_for_workers()

        self.shut_down()

    def link(self, *args, **kwargs):
        return self.greenlet.link(*args, **kwargs)
