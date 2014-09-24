import eventlet
from eventlet import Timeout

from mock import Mock, call

from nameko.containers import ServiceContainer
from nameko.timer import TimerProvider
from nameko.testing.utils import wait_for_call


def test_provider():
    container = Mock(spec=ServiceContainer)
    container.service_name = "service"
    container.config = Mock()
    container.spawn_managed_thread = eventlet.spawn

    timer = TimerProvider(interval=0)
    timer.bind('foobar', container)
    timer.prepare()

    assert timer.interval == 0

    timer.start()

    with wait_for_call(1, container.spawn_worker) as spawn_worker:
        with Timeout(1):
            timer.stop()

    # the timer should have stopped and should only have spawned
    # a single worker
    spawn_worker.assert_called_once_with(timer, (), {})

    assert timer.gt.dead


def test_provider_interval_as_callable():
    container = Mock(spec=ServiceContainer)
    container.service_name = "service"
    container.config = {}

    get_interval = Mock()
    get_interval.return_value = 1

    timer = TimerProvider(interval=get_interval)
    timer.bind('foobar', container)
    timer.prepare()

    assert timer.interval == 1
    assert get_interval.call_args_list == [call(container)]


def test_stop_timer_immediatly():
    container = Mock(spec=ServiceContainer)
    container.service_name = "service"
    container.config = {}

    timer = TimerProvider(interval=5)
    timer.bind('foobar', container)
    timer.prepare()
    timer.start()
    eventlet.sleep(0.1)
    timer.stop()

    assert container.spawn_worker.call_count == 0
    assert timer.gt.dead


def test_kill_stops_timer():
    container = Mock(spec=ServiceContainer)
    container.service_name = "service"
    container.spawn_managed_thread = eventlet.spawn

    timer = TimerProvider(interval=0)
    timer.bind('foobar', container)
    timer.prepare()
    timer.start()

    with wait_for_call(1, container.spawn_worker):
        timer.kill()

    # unless the timer is dead, the following nap would cause a timer
    # to trigger
    eventlet.sleep(0.1)
    assert container.spawn_worker.call_count == 1
