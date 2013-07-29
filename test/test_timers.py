import pytest
import eventlet
from mock import Mock

from nameko.timer import Timer, TimerProvider, timer
from nameko.testing.utils import wait_for_call


class Foobar(object):
    def __init__(self, raise_at_call=0):
        self.timer_calls = 0
        self.raise_at_call = raise_at_call

    @timer(interval=0)
    def foobar(self):
        self.timer_calls += 1
        if self.timer_calls == self.raise_at_call:
            raise FooError('error in call: %d' % self.timer_calls)


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


def test_provider():

    tmrprov = TimerProvider(0)
    container = Mock()
    srv_ctx = {'container': container}
    tmrprov.start()

    timer = tmrprov.timers_by_ctx[srv_ctx]
    assert timer.interval == 5

    tmrprov.on_container_started(srv_ctx)

    with wait_for_call(container.spawn_worker) as spawn_worker:
        spawn_worker.assert_called_once_with()



    get_timer(foo.foobar).interval = 5
    tmr = get_timer(foo.foobar)
    assert tmr.interval == 5


def test_stop_running_timer():
    handler = Mock()

    timer = Timer(0, handler)
    timer.start()

    with eventlet.Timeout(0.5):
        while handler.call_count < 5:
            eventlet.sleep()
        count = handler.call_count

        timer.stop()
        eventlet.sleep()

    assert handler.call_count == count


def test_stop_timer_immediatly():
    handler = Mock()
    timer = Timer(5, handler)
    timer.start()
    eventlet.sleep(0.1)
    timer.stop()
    assert handler.call_count == 1


def test_exception_in_timer_method_ignored():
    handler = Mock()
    handler.side_effect = FooError

    timer = Timer(0, handler)
    timer.start()

    with eventlet.Timeout(0.5):
        while handler.call_count < 5:
            eventlet.sleep()

    assert handler.call_count >= 5
