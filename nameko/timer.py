from __future__ import absolute_import
from functools import partial
from logging import getLogger
from weakref import WeakKeyDictionary
import time
import inspect

import eventlet

_log = getLogger(__name__)


intervals = WeakKeyDictionary()

timers = WeakKeyDictionary()


def set_interval(timer_method, interval):
    _log.debug(
        'update timer for %s with %ss interval',
        timer_method, interval)
    timers[timer_method].interval = interval


def timer(interval, func=None):
    '''
    Decorates a method as a timer which is called every `interval` seconds.

    Example::

        @timer(interval=5)
        def handle_timer(self):
            self.shrub(body)
    '''

    if func is None:
        return partial(timer, interval)
    else:
        intervals[func] = interval
        return func


class Timer(object):
    def __init__(self, interval, func):
        self.interval = interval
        self.func = func
        self.gt = None

    def start(self):
        self.gt = eventlet.spawn(self.run)
        _log.debug(
            'started timer for %s with %ss interval',
            self.func, self.interval)

    def run(self):
        while True:
            start = time.time()
            try:
                self.func()
            except Exception as e:
                _log.error('error in timer handler: %s', e)

            sleep_time = max(self.interval - (time.time() - start), 0)
            eventlet.sleep(sleep_time)


def get_timers(service):
    for name, timer_method in inspect.getmembers(service, inspect.ismethod):
        try:
            interval = intervals[timer_method.im_func]
            tmr = Timer(interval, timer_method)
            timers[timer_method] = tmr
            yield tmr
        except KeyError:
            pass
