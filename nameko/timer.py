from __future__ import absolute_import

import time
from logging import getLogger

from eventlet import Timeout
from eventlet.event import Event

from nameko.extensions import Entrypoint

_log = getLogger(__name__)


class Timer(Entrypoint):
    def __init__(self, interval):
        """
        Timer entrypoint implementation. Fires every :attr:`self.interval`
        seconds.

        The implementation sleeps first, i.e. does not fire at time 0.

        Example::

            timer = Timer.decorator

            class Service(object):
                name = "service"

                @timer(interval=5)
                def tick(self):
                    pass

        """
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
        """ Runs the interval loop. """

        sleep_time = self.interval

        while True:
            # sleep for `sleep_time`, unless `should_stop` fires, in which
            # case we leave the while loop and stop entirely
            with Timeout(sleep_time, exception=False):
                self.should_stop.wait()
                break

            start = time.time()

            self.handle_timer_tick()

            elapsed_time = (time.time() - start)

            # next time, sleep however long is left of our interval, taking
            # off the time we took to run
            sleep_time = max(self.interval - elapsed_time, 0)

    def handle_timer_tick(self):
        args = ()
        kwargs = {}

        # Note that we don't catch ContainerBeingKilled here. If that's raised,
        # there is nothing for us to do anyway. The exception bubbles, and is
        # caught by :meth:`Container._handle_thread_exited`, though the
        # triggered `kill` is a no-op, since the container is alredy
        # `_being_killed`.
        self.container.spawn_worker(self, args, kwargs)


timer = Timer.decorator
