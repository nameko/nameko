import eventlet
from nameko.timer import timer, get_timers, get_timer, Timer


class FooError(Exception):
    pass


class Foobar(object):
    def __init__(self, raise_at_call=0):
        self.timer_calls = 0
        self.raise_at_call = raise_at_call

    @timer(interval=0)
    def foobar(self):
        self.timer_calls += 1
        if self.timer_calls == self.raise_at_call:
            raise FooError('error in call: %d' % self.timer_calls)


def test_get_timers():
    foo = Foobar()
    timers = list(get_timers(foo))

    assert len(timers) == 1
    tmr = timers[0]

    assert isinstance(tmr, Timer)
    assert tmr.interval == 0
    assert tmr.func == foo.foobar


def test_set_interval():
    foo = Foobar()
    get_timer(foo.foobar).interval = 5
    tmr = get_timer(foo.foobar)
    assert tmr.interval == 5


def test_stop_running_timer():
    foo = Foobar()
    tmr = get_timer(foo.foobar)
    tmr.start()

    with eventlet.Timeout(0.5):
        while foo.timer_calls < 5:
            eventlet.sleep()

    count = foo.timer_calls
    tmr.stop()
    assert foo.timer_calls == count


def test_stop_timer_immediatly():
    foo = Foobar()
    tmr = get_timer(foo.foobar)
    tmr.interval = 5
    tmr.start()
    tmr.stop()
    assert foo.timer_calls == 0


def test_exception_in_timer_method_ignored():
    foo = Foobar(2)
    tmr = get_timer(foo.foobar)
    tmr.start()
    with eventlet.Timeout(0.5):
        while foo.timer_calls < 5:
            eventlet.sleep()

    assert foo.timer_calls >= 5
