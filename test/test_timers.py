import eventlet
from eventlet import Timeout
from mock import create_autospec

from nameko.containers import ServiceContainer
from nameko.testing.utils import get_extension, wait_for_call
from nameko.timer import Timer, timer


def spawn_managed_thread(fn, identifier=None):
    return eventlet.spawn(fn)


def test_provider():
    container = create_autospec(ServiceContainer)
    container.service_name = "service"
    container.spawn_managed_thread = spawn_managed_thread

    timer = Timer(interval=0.1).bind(container, "method")
    timer.setup()
    timer.start()

    assert timer.interval == 0.1

    with wait_for_call(1, container.spawn_worker) as spawn_worker:
        with Timeout(1):
            timer.stop()

    # the timer should have stopped and should only have spawned
    # a single worker
    spawn_worker.assert_called_once_with(timer, (), {})
    assert timer.gt.dead


def test_stop_timer_immediately():
    container = create_autospec(ServiceContainer)
    container.service_name = "service"
    container.config = {}

    timer = Timer(interval=5).bind(container, "method")
    timer.setup()
    timer.start()

    eventlet.sleep(0.1)
    timer.stop()

    assert container.spawn_worker.call_count == 0
    assert timer.gt.dead


def test_kill_stops_timer():
    container = create_autospec(ServiceContainer)
    container.service_name = "service"
    container.spawn_managed_thread = spawn_managed_thread

    timer = Timer(interval=0).bind(container, "method")
    timer.setup()
    timer.start()

    with wait_for_call(1, container.spawn_worker):
        timer.kill()

    # unless the timer is dead, the following nap would cause a timer
    # to trigger
    eventlet.sleep(0.1)
    assert container.spawn_worker.call_count == 1


class TestEntrypointArguments:

    def test_expected_exceptions_and_sensitive_arguments(self, container_factory):

        class Boom(Exception):
            pass

        class Service(object):
            name = "service"

            @timer(1, expected_exceptions=Boom, sensitive_arguments=["arg"])
            def method(self, arg):
                pass  # pragma: no cover

        container = container_factory(Service)
        container.start()

        entrypoint = get_extension(container, Timer)
        assert entrypoint.expected_exceptions == Boom
        assert entrypoint.sensitive_arguments == ["arg"]
