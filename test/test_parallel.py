from greenlet import GreenletExit
import eventlet
from mock import Mock, MagicMock
import pytest
from nameko.dependencies import get_injection_providers
from nameko.parallel import ParallelExecutor, parallel_executor, \
    ParallelProvider, ParallelExecutorBusyException
from nameko.service import ManagedThreadContainer, ServiceContainer, \
    WorkerContext, ServiceRunner
from nameko.testing.utils import wait_for_call
from nameko.timer import timer


def test_parallel_executor_gives_submit():
    to_call = Mock(return_value=99)
    future = ParallelExecutor(ManagedThreadContainer()).submit(to_call, 1)
    with wait_for_call(5, to_call) as to_call_waited:
        to_call_waited.assert_called_with(1)
        assert future.result() == 99


def test_calling_result_waits():
    to_call = Mock(return_value=99)
    future = ParallelExecutor(ManagedThreadContainer()).submit(to_call, 1)
    assert future.result() == 99
    to_call.assert_called_with(1)


def test_parallel_wrapper_layer():
    to_wrap = Mock()
    to_wrap.wrapped_attribute = 2
    to_call = Mock()
    to_wrap.wrapped_call = to_call
    executor = ParallelExecutor(ManagedThreadContainer())

    # Confirm wrapped call comes via submit
    orig_submit = executor.submit

    submit_check = Mock()

    def pass_through_submit(*args, **kwargs):
        submit_check()
        orig_submit(*args, **kwargs)

    executor.submit = pass_through_submit

    # Non-callables are returned immediately
    assert executor(to_wrap).wrapped_attribute == 2

    # Callables must be waited for
    executor(to_wrap).wrapped_call(3)

    with wait_for_call(5, to_call) as to_call_waited:
        to_call_waited.assert_called_with(3)
    submit_check.assert_called_with()


def test_parallel_executor_context_manager():
    to_call = Mock()
    with ParallelExecutor(ManagedThreadContainer()) as execution_context:
        execution_context.submit(to_call, 4)
    # No waiting, the context manager handles that
    to_call.assert_called_with(4)


def test_parallel_executor_wrapped_cm():
    to_wrap = Mock()
    to_call = Mock()
    to_wrap.wrapped_call = to_call
    with ParallelExecutor(ManagedThreadContainer())(to_wrap) as wrapped:
        wrapped.wrapped_call(5)
    # No waiting, the context manager handles that
    to_call.assert_called_with(5)


def test_no_submit_after_shutdown():
    pe = ParallelExecutor(ManagedThreadContainer())
    to_call = Mock()
    with pe as execution_context:
        execution_context.submit(to_call, 1)
    with pytest.raises(RuntimeError):
        pe.submit(to_call, 2)


def test_future_gets_exception():
    pe = ParallelExecutor(ManagedThreadContainer())

    def raises():
        raise Exception()

    future = pe.submit(raises)

    with pytest.raises(Exception):
        future.result()


def everlasting_call():
    while True:
        eventlet.sleep(1)


class ExampleService(object):
    injected = parallel_executor()

    @timer(interval=1)
    def handle_timer(self):
        self.injected.submit(everlasting_call)


def test_parallel_executor_injection():
    config = Mock()
    container = ServiceContainer(ExampleService, WorkerContext, config)

    providers = list(get_injection_providers(container))
    assert len(providers) == 1
    provider = providers[0]

    assert provider.name == "injected"
    assert isinstance(provider, ParallelProvider)


def test_busy_check_on_teardown():
    # max_workers needs to be provided, as it's used in a semaphore count
    config = MagicMock({'max_workers': 4})
    kill_called = Mock()

    class MockedContainer(ServiceContainer):
        def kill(self, exc):
            kill_called(type(exc))
            super(MockedContainer, self).kill(exc)

    sr = ServiceRunner(config, container_cls=MockedContainer)
    sr.add_service(ExampleService)
    sr.start()
    with wait_for_call(5, kill_called) as kill_called_waited:
        kill_called_waited.assert_called_with(ParallelExecutorBusyException)
    with pytest.raises(GreenletExit):
        sr.stop()
