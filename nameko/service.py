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
from nameko.logging import log_time
from nameko.messaging import get_consumers
from nameko.dependencies import inject_dependencies
from nameko.sending import process_rpc_message
from nameko.timer import get_timers


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
        self.connection_factory = connection_factory

        inject_dependencies(self.controller, self)

        self._connection_pool = Pool(
            max_size=self.procpool.size,
            create=connection_factory
        )

        self.workers = set()
        self._pending_ack_messages = []
        self._pending_requeue_messages = []
        self._do_cancel_consumers = False
        self._consumers_cancelled = Event()

        self._timers = list(get_timers(self.controller))

    def start(self):
        self.start_timers()
        # greenlet has a magic attribute ``dead`` - pylint: disable=E1101
        if self.greenlet is not None and not self.greenlet.dead:
            raise RuntimeError()
        self.greenlet = eventlet.spawn(self.run)

    def start_timers(self):
        for timer in self._timers:
            timer.start()

    def get_consumers(self, Consumer, channel):
        nova_consumer = Consumer(
            self.nova_queues, callbacks=[self.on_nova_message, ])

        consume_consumers = get_consumers(
            Consumer, self, self.on_consume_message)

        consumers = [nova_consumer] + list(consume_consumers)

        prefetch_count = self.procpool.size
        for consumer in consumers:
            consumer.qos(prefetch_count=prefetch_count)

        return consumers

    def on_consume_ready(self, connection, channel, consumers, **kwargs):
        self._consumers = consumers
        self._channel = channel
        self.consume_ready.send(None)

    def on_consume_end(self, connection, channel):
        self.consume_ready.reset()

    def on_nova_message(self, body, message):
        _log.debug('spawning RPC worker (%d free)', self.procpool.free())

        gt = self.procpool.spawn(self.handle_rpc_message, body)

        gt.link(self.handle_rpc_message_processed, message)
        self.workers.add(gt)

    def on_consume_message(self, consumer_method_config, body, message):
        _log.debug('spawning consume worker (%d free)', self.procpool.free())

        gt = self.procpool.spawn(
            self.handle_consume_message, consumer_method_config, body, message)

        gt.link(self.handle_consume_message_processed)
        self.workers.add(gt)

    def handle_rpc_message(self, body):
        # item is patched on for python with ``with``, pylint can't find it
        # pylint: disable=E1102
        with self._connection_pool.item() as connection:
            process_rpc_message(connection, self.controller, body)

    def handle_rpc_message_processed(self, gt, message):
        self.workers.discard(gt)
        self._pending_ack_messages.append(message)

    def handle_consume_message(self, consumer_method_config, body, message):
        with log_time(_log.debug, 'processed consume message in %0.3fsec'):
            consumer_method, consumer_config = consumer_method_config

            try:
                consumer_method(body)
            except Exception as e:
                if consumer_config.requeue_on_error:
                    _log.exception(
                        'failed to consume message, requeueing message: '
                        '%s(): %s', consumer_method, e)
                    self._pending_requeue_messages.append(message)
                else:
                    _log.exception(
                        'failed to consume message, ignoring message: '
                        '%s(): %s', consumer_method, e)
                    self._pending_ack_messages.append(message)
            else:
                self._pending_ack_messages.append(message)

    def handle_consume_message_processed(self, gt):
        self.workers.discard(gt)

    def on_iteration(self):
        self.process_consumer_cancellation()
        # we need to make sure we process any pending messages before shutdown
        self.process_pending_message_acks()
        self.process_shutdown()

    def process_consumer_cancellation(self):
        if self._do_cancel_consumers:
            self._do_cancel_consumers = False
            if self._consumers:
                _log.debug('cancelling consumers')
                for consumer in self._consumers:
                    consumer.cancel()
            self._consumers_cancelled.send(True)

    def process_pending_message_acks(self):
        messages = self._pending_ack_messages
        if messages:
            _log.debug('ack() %d processed messages', len(messages))
            while messages:
                msg = messages.pop()
                msg.ack()
                eventlet.sleep()

        messages = self._pending_requeue_messages
        if messages:
            _log.debug('requeue() %d processed messages', len(messages))
            while messages:
                msg = messages.pop()
                msg.requeue()
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
                        # Excluding the following clause from coverage,
                        # as timeout never appears to be set - This method
                        # is a lift from kombu so will leave in place for now.
                        if timeout and elapsed >= timeout:  # pragma: no cover
                            raise socket.timeout()
                    except socket.error:
                        if not self.should_stop:
                            raise
                    else:
                        yield
                        elapsed = 0

    def process_shutdown(self):
        consumers_cancelled = self._consumers_cancelled.ready()

        no_active_timers = (len(self._timers) == 0)

        no_active_workers = (self.procpool.running() < 1)

        no_pending_message_acks = not (
            self._pending_ack_messages or
            self._pending_requeue_messages
        )

        ready_to_stop = (
            consumers_cancelled and
            no_active_timers and
            no_active_workers and
            no_pending_message_acks
        )

        if ready_to_stop:
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

    def cancel_timers(self):
        if self._timers:
            _log.debug('stopping %d timers', len(self._timers))
            while self._timers:
                self._timers.pop().stop()

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

        self.cancel_timers()

        if force:
            self.kill_workers()
        else:
            self.wait_for_workers()

        self.shut_down()

    def link(self, *args, **kwargs):
        return self.greenlet.link(*args, **kwargs)
