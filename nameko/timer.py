from __future__ import absolute_import
from logging import getLogger
import time
from weakref import WeakKeyDictionary

import eventlet
from eventlet import Timeout
from eventlet.event import Event

from nameko.dependencies import (
    dependency_decorator, get_providers, DependencyProvider)

_log = getLogger(__name__)


@dependency_decorator
def timer(interval):
    '''
    Decorates a method as a timer, which will be called every `interval` sec.

    Example:

    class Foobar(object):

        @timer(interval=5)
        def handle_timer(self):
            self.shrub(body)
    '''
    return TimerProvider(interval)


class TimerProvider(DependencyProvider):
    def __init__(self, interval):
        self.timers_by_ctx = WeakKeyDictionary()
        self.interval = None

    def start(self, srv_ctx):
        def handler():
            srv_ctx['container'].spawn_worker(self.name)

        self.timers_by_ctx[srv_ctx] = Timer(self.interval, handler)

    def on_container_started(self, srv_ctx):
        timer = self.timers_by_ctx[srv_ctx]
        _log.debug(
            'started timer for %s with %ss interval',
            self.name, timer.interval)
        timer.start()

    def stop(self, srv_ctx):
        self.timers_by_ctx[srv_ctx].stop()


class Timer(object):
    ''' A timer object, which will call a given method repeatedly at a given
    interval.
    '''
    def __init__(self, interval, handler):
        self.interval = interval
        self.gt = None
        self.should_stop = Event()
        self.handler = handler

    def start(self):
        ''' Starts the timer in a separate green thread.

        Once started it may be stopped using its `stop()` method.
        '''
        self.gt = eventlet.spawn(self._run)

    def stop(self):
        ''' Gracefully stops the timer, waiting for it's timer_method
        to complete if it is running.
        '''
        self.should_stop.send(True)
        self.gt.wait()

    def _run(self):
        ''' Runs the interval loop.

        This should not be called directly, rather the `start()` method
        should be used.
        '''
        while not self.should_stop.ready():
            start = time.time()
            try:
                self.handler()
            except Exception as e:
                _log.error('error in timer handler: %s', e)

            sleep_time = max(self.interval - (time.time() - start), 0)
            self._sleep_or_stop(sleep_time)

    def _sleep_or_stop(self, sleep_time):
        ''' Sleeps for `sleep_time` seconds or until a `should_stop` event
        has been fired, whichever comes first.
        '''
        try:
            with Timeout(sleep_time):
                self.should_stop.wait()
        except Timeout:
            # we use the timeout as a cancellable sleep
            pass

