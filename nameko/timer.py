from __future__ import absolute_import
from logging import getLogger
import time

import eventlet
from eventlet import Timeout
from eventlet.event import Event

from nameko.dependencies import dependency_decorator, get_providers

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
    return Timer(interval)


class Timer(object):
    ''' A timer object, which will call a given method repeatedly at a given
    interval.
    '''
    def __init__(self, interval):
        self.interval = interval

    def container_init(self, container, handler_name):
        self.gt = None
        self.should_stop = Event()
        self.container = container
        self.handler_name = handler_name

    def container_start(self):
        ''' Starts the timer in a separate green thread.

        Once started it may be stopped using its `stop()` method.
        '''
        self.gt = eventlet.spawn(self._run)
        _log.debug(
            'started timer for %s with %ss interval',
            self.handler_name, self.interval)

    def container_stop(self):
        self.stop()

    def _run(self):
        ''' Runs the interval loop.

        This should not be called directly, rather the `start()` method
        should be used.
        '''
        while not self.should_stop.ready():
            start = time.time()
            try:
                # TODO: this needs to tell the container to start a worker
                #       and call the method
                fn = getattr(self.container.controller, self.handler_name)
                fn()
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

    def stop(self):
        ''' Gracefully stops the timer, waiting for it's timer_method
        to complete if it is running.
        '''
        self.should_stop.send(True)
        self.gt.wait()


class NoTimerForMethdod(Exception):
    pass


def get_timer(timer_method):
    ''' Returns the timer for a `@timer` decorated method.

    It will raise a NoTimerForMethdod if the `timer_method` was not
    decorated with `@timer`.
    '''

    try:
        return next(get_providers(timer_method, Timer))
    except StopIteration:
        raise NoTimerForMethdod()
