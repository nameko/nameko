from __future__ import absolute_import
from logging import getLogger
import time

from eventlet import Timeout
from eventlet.event import Event

from nameko.dependencies import Entrypoint

_log = getLogger(__name__)


class Timer(Entrypoint):
    def __init__(self, interval=None, config_key=None):
        """
        Timer entrypoint implementation. Fires every :attr:`self.interval`
        seconds.

        Either `interval` or `config_key` must be provided. If given,
        `config_key` specifies a key in the config will be used as the
        interval.

        Example::

            timer = Timer.entrypoint

            class Foobar(object):

                @timer(interval=5)
                def tick(self):
                    self.shrub(body)

                @timer(config_key='tock_interval')
                def tock(self):
                    self.shrub(body)
        """
        self._default_interval = interval
        self.config_key = config_key
        self.should_stop = Event()
        self.gt = None

    def setup(self, container):
        self.container = container  # stash container (TEMP?)

    def start(self):
        _log.debug('starting %s', self)
        interval = self._default_interval

        if self.config_key:
            interval = self.container.config.get(self.config_key, interval)

        self.interval = interval
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
        args = ()
        kwargs = {}

        # Note that we don't catch ContainerBeingKilled here. If that's raised,
        # there is nothing for us to do anyway. The exception bubbles, and is
        # caught by :meth:`Container._handle_thread_exited`, though the
        # triggered `kill` is a no-op, since the container is alredy
        # `_being_killed`.
        self.container.spawn_worker(self, args, kwargs)


# backwards compat
TimerProvider = Timer

timer = Timer.entrypoint
