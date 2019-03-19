import logging
import time

import eventlet
import pytest
from mock import Mock

from nameko.testing.services import entrypoint_hook, get_extension
from nameko.testing.utils import wait_for_call
from nameko.timer import Timer, timer


@pytest.fixture
def tracker():
    return Mock()


@pytest.mark.parametrize('interval,eager,call_duration,expected_calls', [
    (0.1, False, 0, 1),
    (0.1, True, 0, 2),
    (5, False, 0, 0),
    (5, True, 0, 1),
    # call duration delays second call
    (0.1, False, 0.3, 1),  # fires at 0.1
    (0.1, True, 0.3, 1),  # fires at 0, 0.2
    (0.025, False, 0.07, 2),  # fires at 0.25, 0.095, 0.165
    (0.025, True, 0.07, 3),  # fires at 0, 0.07, 0.14
])
def test_timer_run(interval, eager, call_duration, expected_calls,
                   container_factory, tracker):
    """Test running the timers main loop.

    We test with "timer_only" mode, where only the main loop is run as well as
    in a more comprehensive mode where the entire container is tested.

    """
    timeout = 0.15
    times = []

    class Service(object):
        name = "service"

        @timer(interval, eager)
        def tick(self):
            times.append(time.time())
            eventlet.sleep(call_duration)
            tracker()

    container = container_factory(Service, {})

    # Check that Timer instance is initialized correctly
    instance = get_extension(container, Timer)
    assert instance.interval == interval
    assert instance.eager == eager

    t0 = time.time()
    container.start()
    eventlet.sleep(timeout)
    container.stop()

    rel_times = [t - t0 for t in times]
    assert tracker.call_count == expected_calls, (
        'Expected {} calls but got {} with '
        '{}timer interval of {} and call duration of {}. '
        'Times were {}'
    ).format(expected_calls, tracker.call_count,
             'eager-' if eager else '', interval, call_duration, rel_times)


def test_kill_stops_timer(container_factory, tracker):

    class Service(object):
        name = "service"

        @timer(0)
        def tick(self):
            tracker()

    container = container_factory(Service, {})
    container.start()

    with wait_for_call(1, tracker):
        container.kill()

    # unless the timer is dead, the following nap would cause a timer
    # to trigger
    eventlet.sleep(0.1)
    assert tracker.call_count == 1


def test_stop_while_sleeping(container_factory, tracker):
    """Check that waiting for the timer to fire does not block the container
    from being shut down gracefully.
    """
    class Service(object):
        name = "service"

        @timer(5)
        def tick(self):
            tracker()  # pragma: no cover

    container = container_factory(Service, {})
    container.start()

    # raise a Timeout if the container fails to stop within 1 second
    with eventlet.Timeout(1):
        container.stop()

    assert tracker.call_count == 0, 'Timer should not have fired.'


def test_timer_error(container_factory, caplog, tracker):
    """Check that an error in the decorated method does not cause the service
    containers loop to raise an exception.
    """

    class Service(object):
        name = "service"

        @timer(5, True)
        def tick(self):
            tracker()

    tracker.side_effect = ValueError('Boom!')
    container = container_factory(Service, {})

    with caplog.at_level(logging.CRITICAL):
        container.start()
        eventlet.sleep(0.05)
        # Check that the function was actually called and that the error was
        # handled gracefully.
        assert tracker.call_count == 1
        container.stop()

    # Check that no errors are thrown in the runners thread.
    # We can't check for raised errors here as the actual
    # exception is eaten by the worker pools handler.
    assert len(caplog.records) == 0, (
        'Expected no errors to have been '
        'raised in the worker thread.'
    )


def test_expected_error_in_worker(container_factory, caplog):
    """Make sure that expected exceptions are processed correctly."""
    class ExampleError(Exception):
        pass

    class Service(object):
        name = "service"

        @timer(1, expected_exceptions=(ExampleError,))
        def tick(self):
            raise ExampleError('boom!')

    container = container_factory(Service, {})

    with entrypoint_hook(container, "tick") as tick:
        container.start()
        with pytest.raises(ExampleError):
            tick()

    assert len(caplog.records) == 1
    assert caplog.records[0].message == (
        '(expected) error handling worker {}: boom!'
    ).format(caplog.records[0].args[0])


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
