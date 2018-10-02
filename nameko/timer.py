from __future__ import absolute_import

import time
from logging import getLogger

from eventlet import Timeout
from eventlet.event import Event

from nameko.extensions import Entrypoint


_log = getLogger(__name__)


class Timer(Entrypoint):
    def __init__(self, interval, eager=False):
        """
        Timer entrypoint. Fires every `interval` seconds or as soon as
        the previous worker completes if that took longer.

        The default behaviour is to wait `interval` seconds
        before firing for the first time. If you want the entrypoint
        to fire as soon as the service starts, pass `eager=True`.

        Example::

            timer = Timer.decorator

            class Service(object):
                name = "service"

                @timer(interval=5)
                def tick(self):
                    pass

        """
        self.interval = interval
        self.eager = eager
        self.should_stop = Event()
        self.worker_complete = Event()
        self.gt = None
        super(Timer, self).__init__()

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
        sleep_time = 0 if self.eager else self.interval

        while True:
            # sleep for `sleep_time`, unless `should_stop` fires, in which
            # case we leave the while loop and stop entirely
            with Timeout(sleep_time, exception=False):
                self.should_stop.wait()
                break

            start = time.time()

            self.handle_timer_tick()

            self.worker_complete.wait()
            self.worker_complete.reset()

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
        # triggered `kill` is a no-op, since the container is already
        # `_being_killed`.
        self.container.spawn_worker(
            self, args, kwargs, handle_result=self.handle_result)

    def handle_result(self, worker_ctx, result, exc_info):
        self.worker_complete.send()
        return result, exc_info


timer = Timer.decorator
