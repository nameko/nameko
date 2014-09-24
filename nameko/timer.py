from __future__ import absolute_import
from logging import getLogger
import time

from eventlet import Timeout
from eventlet.event import Event

from nameko.dependencies import (
    entrypoint, EntrypointProvider, DependencyFactory, ConfigValue)

_log = getLogger(__name__)


@entrypoint
def timer(interval=None):
    '''
    Decorates a method as a timer, which will be called every `interval` sec.

    :parameters:
        interval : int or callable
            Interval in seconds between timer 'ticks', or a callable that
            will return a suitable value when invoked with active
            :class:`nameko.containers.ServiceContainer`.

    Example::

        def get_interval(container):
            return container.config.get('TIMER_TICK')

        class Foobar(object):

            @timer(interval=get_interval)
            def tick(self):
                pass

            @timer(interval=5)
            def tock(self):
                self.shrub(body)
    '''
    return DependencyFactory(TimerProvider, interval)


class TimerProvider(EntrypointProvider):

    interval = ConfigValue(None)

    def __init__(self, interval):
        self.interval = interval
        self.should_stop = Event()
        self.gt = None

    def start(self):
        _log.debug('starting %s', self)
        self.gt = self.container.spawn_managed_thread(self._run)

    def stop(self):
        _log.debug('stopping %s', self)
        self.should_stop.send(True)
        self.gt.wait()

    def kill(self):
        _log.debug('killing %s', self)
        self.gt.kill()

    def _run(self):
        ''' Runs the interval loop.

        This should not be called directly, rather the `start()` method
        should be used.
        '''
        while not self.should_stop.ready():
            start = time.time()

            self.handle_timer_tick()

            elapsed_time = (time.time() - start)
            sleep_time = max(self.interval - elapsed_time, 0)
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

    def handle_timer_tick(self):
        args = tuple()
        kwargs = {}

        # Note that we don't catch ContainerBeingKilled here. If that's raised,
        # there is nothing for us to do anyway. The exception bubbles, and is
        # caught by :meth:`Container._handle_thread_exited`, though the
        # triggered `kill` is a no-op, since the container is alredy
        # `_being_killed`.
        self.container.spawn_worker(self, args, kwargs)
