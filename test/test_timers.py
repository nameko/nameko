import eventlet
from eventlet import Timeout

from mock import Mock

from nameko.containers import ServiceContainer
from nameko.timer import TimerProvider
from nameko.testing.utils import wait_for_call


def test_provider():
    container = Mock(spec=ServiceContainer)
    container.service_name = "service"
    container.config = Mock()
    container.spawn_managed_thread = eventlet.spawn

    timer = TimerProvider(interval=0, config_key=None)
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


def test_provider_uses_config_for_interval():
    container = Mock(spec=ServiceContainer)
    container.service_name = "service"
    container.config = {'spam-conf': 10}
    container.spawn_managed_thread = eventlet.spawn

    timer = TimerProvider(interval=None, config_key='spam-conf')
    timer.bind('foobar', container)
    timer.prepare()

    assert timer.interval == 10


def test_provider_interval_as_config_fallback():
    container = Mock(spec=ServiceContainer)
    container.service_name = "service"
    container.config = {}

    timer = TimerProvider(interval=1, config_key='spam-conf')
    timer.bind('foobar', container)
    timer.prepare()

    assert timer.interval == 1


def test_stop_timer_immediatly():
    container = Mock(spec=ServiceContainer)
    container.service_name = "service"
    container.config = {}

    timer = TimerProvider(interval=5, config_key=None)
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

    timer = TimerProvider(interval=0, config_key=None)
    timer.bind('foobar', container)
    timer.prepare()
    timer.start()

    with wait_for_call(1, container.spawn_worker):
        timer.kill()

    # unless the timer is dead, the following nap would cause a timer
    # to trigger
    eventlet.sleep(0.1)
    assert container.spawn_worker.call_count == 1
