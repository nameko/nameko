import eventlet
from eventlet import Timeout

from mock import Mock

from nameko.timer import Timer, TimerProvider
from nameko.service import ServiceContext, ServiceContainer
from nameko.testing.utils import wait_for_call


def test_provider():

    srv_ctx = ServiceContext('foo', None, None)
    service_container = Mock(spec=ServiceContainer)
    service_container.ctx = srv_ctx

    tmrprov = TimerProvider(interval=0, config_key=None)
    tmrprov.bind('foobar', service_container)
    tmrprov.prepare()

    timer = tmrprov.timers_by_ctx[srv_ctx]
    assert timer.interval == 0

    tmrprov.start()

    with wait_for_call(1, service_container.spawn_worker) as spawn_worker:
        spawn_worker.assert_called_once_with(tmrprov, (), {})

    with Timeout(1):
        tmrprov.stop()

    assert timer.gt.dead


def test_provider_uses_config_for_interval():

    srv_ctx = ServiceContext('foo', None, None, {'spam-conf': 10})
    service_container = Mock(spec=ServiceContainer)
    service_container.ctx = srv_ctx

    tmrprov = TimerProvider(interval=None, config_key='spam-conf')
    tmrprov.bind('foobar', service_container)
    tmrprov.prepare()

    timer = tmrprov.timers_by_ctx[srv_ctx]
    assert timer.interval == 10


def test_provider_interval_as_config_fallback():

    srv_ctx = ServiceContext('foo', None, None, {})
    service_container = Mock(spec=ServiceContainer)
    service_container.ctx = srv_ctx

    tmrprov = TimerProvider(interval=1, config_key='spam-conf')
    tmrprov.bind('foobar', service_container)
    tmrprov.prepare()

    timer = tmrprov.timers_by_ctx[srv_ctx]
    assert timer.interval == 1


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
    assert timer.gt.dead


def test_stop_timer_immediatly():
    handler = Mock()
    timer = Timer(5, handler)
    timer.start()
    eventlet.sleep(0.1)
    timer.stop()
    assert handler.call_count == 1
    assert timer.gt.dead


def test_exception_in_timer_method_ignored():
    handler = Mock()
    handler.side_effect = Exception('foo')

    timer = Timer(0, handler)
    timer.start()

    with eventlet.Timeout(0.5):
        while handler.call_count < 5:
            eventlet.sleep()

    assert handler.call_count >= 5
    timer.stop()
