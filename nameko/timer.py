from __future__ import absolute_import
from logging import getLogger
from weakref import WeakKeyDictionary
import time
import inspect

import eventlet
from eventlet import Timeout
from eventlet.event import Event

_log = getLogger(__name__)


intervals = WeakKeyDictionary()
timers = WeakKeyDictionary()


def timer(interval):
    '''
    Decorates a method as a timer, which will be called every `interval` sec.

    Example:

    class Foobar(object):

        @timer(interval=5)
        def handle_timer(self):
            self.shrub(body)
    '''

    def timer_decorator(func):
        intervals[func] = interval
        return func

    return timer_decorator


class Timer(object):
    ''' A timer object, which will call a given method repeatedly at a given
    interval.
    '''
    def __init__(self, interval, func):
        self.interval = interval
        self.func = func
        self.gt = None
        self.should_stop = Event()

    def start(self):
        ''' Starts the timer in a separate green thread.

        Once started it may be stopped using its `stop()` method.
        '''
        self.gt = eventlet.spawn(self._run)
        _log.debug(
            'started timer for %s with %ss interval',
            self.func, self.interval)

    def _run(self):
        ''' Runs the interval loop.

        This should not be called directly, rather the `start()` method
        should be used.
        '''
        while not self.should_stop.ready():
            start = time.time()
            try:
                self.func()
            except Exception as e:
                _log.exception('error in timer handler: %s', e)

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


def get_interval(timer_method):
    ''' Returns the initial interval set for the decorated `timer_method`.

    It will raise a NoTimerForMethdod if the `timer_method` was not
    decorated with `@timer`.
    '''
    try:
        return intervals[timer_method.im_func]
    except KeyError:
        raise NoTimerForMethdod(timer_method)


def get_timer(timer_method):
    ''' Returns the timer for a `@timer` decorated method.

    It will raise a NoTimerForMethdod if the `timer_method` was not
    decorated with `@timer`.
    '''
    try:
        tmr = timers[timer_method]
    except KeyError:
        interval = get_interval(timer_method)
        tmr = Timer(interval, timer_method)
        timers[timer_method] = tmr

    return tmr


def get_timers(service):
    ''' Returns all the timers of all the methods decorated with `@timer`
    on the `service` object.
    '''
    for _, timer_method in inspect.getmembers(service, inspect.ismethod):
        try:
            yield get_timer(timer_method)
        except NoTimerForMethdod:
            pass
