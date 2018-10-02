import logging
import time

import eventlet
import pytest
from mock import Mock

from nameko.testing.services import get_extension
from nameko.testing.utils import wait_for_call
from nameko.timer import Timer


@pytest.mark.parametrize('timer_only', [False, True])
@pytest.mark.parametrize('interval,eager,call_duration,expected_calls', [
    (0.1, False, 0, 1),
    (0.1, True, 0, 2),
    (5, False, 0, 0),
    (5, True, 0, 1),
    # call duration delays second call
    (0.1, False, 0.2, 1),  # fires at 0.1
    (0.1, True, 0.2, 1),  # fires at 0, 0.2
    (0.025, False, 0.07, 2),  # fires at 0.25, 0.095, 0.165
    (0.025, True, 0.07, 3),  # fires at 0, 0.07, 0.14
])
def test_timer_run(interval, eager, call_duration, expected_calls,
                   timer_only, container_factory):
    """Test running the timers main loop.

    We test with "timer_only" mode, where only the main loop is run as well as
    in a more comprehensive mode where the entire container is tested.

    """
    timeout = 0.15
    Service = _get_timer_test_service(interval, eager)
    container = container_factory(Service, {})

    times = []

    def side_effect():
        times.append(time.time())
        eventlet.sleep(call_duration)

    Service.fcn.side_effect = side_effect
    timer = get_extension(container, Timer)
    assert timer.interval == interval
    assert timer.eager == eager

    t0 = time.time()
    if timer_only:
        timer.gt = Mock()
        with eventlet.Timeout(timeout, exception=False):
            timer._run()
    else:
        container.start()
        eventlet.sleep(0.15)
        container.stop()

    rel_times = [t - t0 for t in times]
    assert Service.fcn.call_count == expected_calls, (
        'Expected {} calls but got {} with '
        '{}timer interval of {} and call duration of {}. '
        'Times were {}'
    ).format(expected_calls, Service.fcn.call_count,
             'eager-' if eager else '', interval, call_duration, rel_times)


def test_kill_stops_timer(container_factory):
    Service = _get_timer_test_service(0, False)
    container = container_factory(Service, {})
    container.start()

    with wait_for_call(1, Service.fcn):
        container.kill()

    # unless the timer is dead, the following nap would cause a timer
    # to trigger
    eventlet.sleep(0.1)
    assert Service.fcn.call_count == 1


def test_stop_while_sleeping(container_factory):
    """Check that waiting for the timer to fire does not block the container
    from being shut down gracefully.
    """
    Service = _get_timer_test_service(5, False)
    container = container_factory(Service, {})
    container.start()

    # raise a Timeout if the container fails to stop within 1 second
    with eventlet.Timeout(1):
        container.stop()


def test_timer_error(container_factory, caplog):
    """Check that an error in the decorated method does not cause the service
    containers loop to raise an exception.
    """
    Service = _get_timer_test_service(5, True)
    Service.fcn.side_effect = ValueError('Boom!')
    container = container_factory(Service, {})

    with caplog.at_level(logging.CRITICAL):
        container.start()
        eventlet.sleep(0.05)
        # Check that the function was actually called and that the error was
        # handled gracefully.
        assert Service.fcn.call_count == 1
        container.stop()

    # Check that no errors are thrown in the runners thread.
    # We can't check for raised errors here as the actual
    # exception is eaten by the worker pools handler.
    assert len(caplog.records) == 0, (
        'Expected no errors to have been '
        'raised in the worker thread.'
    )


def _get_timer_test_service(interval, eager, fcn=None):
    """Construct a dummy nameko service with a simple timer entrypoint."""
    fcn = fcn or Mock()

    class Service(object):
        name = "service"

        @Timer.decorator(interval, eager)
        def tick(self):
            return fcn()

    Service.fcn = fcn
    return Service
