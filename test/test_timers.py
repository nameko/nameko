import pytest
import eventlet
from mock import Mock

from nameko.timer import timer, get_timer, NoTimerForMethdod
from nameko.dependencies import get_decorator_providers


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


def test_get_timer_fails():
    with pytest.raises(NoTimerForMethdod):
        get_timer(test_get_timer_fails)


def test_set_interval():
    foo = Foobar()
    get_timer(foo.foobar).interval = 5
    tmr = get_timer(foo.foobar)
    assert tmr.interval == 5


def test_stop_running_timer():
    foo = Foobar()
    container = Mock()
    container.controller = foo

    for name, tmr in get_decorator_providers(foo):
        tmr.interval = 0
        tmr.container_init(container, name)

        tmr.container_start()

        with eventlet.Timeout(0.5):
            while foo.timer_calls < 5:
                eventlet.sleep()

        count = foo.timer_calls
        tmr.stop()

    assert foo.timer_calls == count


def test_stop_timer_immediatly():
    foo = Foobar()
    container = Mock()
    container.controller = foo

    for name, tmr in get_decorator_providers(foo):
        tmr.interval = 5
        tmr.container_init(container, name)
        tmr.container_start()
        eventlet.sleep(0.1)
        tmr.stop()

    assert foo.timer_calls == 1


def test_exception_in_timer_method_ignored():
    foo = Foobar(2)
    container = Mock()
    container.controller = foo

    for name, tmr in get_decorator_providers(foo):
        tmr.interval = 0
        tmr.container_init(container, name)

        tmr.container_start()

        with eventlet.Timeout(0.5):
            while foo.timer_calls < 5:
                eventlet.sleep()

    assert foo.timer_calls >= 5
