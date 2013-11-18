from greenlet import GreenletExit
import eventlet
from mock import Mock, MagicMock
import pytest
from nameko.dependencies import get_injection_providers
from nameko.parallel import (
    ParallelExecutor, parallel_provider, ParallelProvider,
    ParallelProxyFactory, ProxySettingUnsupportedException)
from nameko.containers import (
    ManagedThreadContainer, ServiceContainer, WorkerContext)
from nameko.runners import ServiceRunner
from nameko.testing.utils import wait_for_call
from nameko.timer import timer


def test_parallel_executor_submit_makes_call():
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


def test_parallel_executor_context_manager():
    to_call = Mock()
    with ParallelExecutor(ManagedThreadContainer()) as execution_context:
        execution_context.submit(to_call, 4)
    # No waiting, the context manager handles that
    to_call.assert_called_with(4)


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
        raise AssertionError()

    future = pe.submit(raises)

    with pytest.raises(AssertionError):
        future.result()


def test_stop_managed_container():
    container = ManagedThreadContainer()
    pe = ParallelExecutor(container)
    with pe as execution_context:
        execution_context.submit(everlasting_call)
        container.stop()


def test_kill_managed_container():
    container = ManagedThreadContainer()
    pe = ParallelExecutor(container)
    with pe as execution_context:
        f = execution_context.submit(everlasting_call)
        container.kill(Exception())
        with pytest.raises(GreenletExit):
            f.result()


def test_parallel_proxy_context_manager():
    to_wrap = Mock()
    to_wrap.wrapped_attribute = 2
    to_call = Mock()
    to_wrap.wrapped_call = to_call
    with ParallelProxyFactory(ManagedThreadContainer())(to_wrap) as wrapped:
        # Non-callables are returned immediately
        assert wrapped.wrapped_attribute == 2

        wrapped.wrapped_call(5)
    # No waiting, the context manager handles that
    to_call.assert_called_with(5)


def test_proxy_read_only():
    class Dummy(object):
        pass

    dummy = Dummy()

    with ParallelProxyFactory(ManagedThreadContainer())(dummy) as wrapped:
        # Setting attributes is not allowed
        with pytest.raises(ProxySettingUnsupportedException):
            wrapped.set_me = 1


def everlasting_call():
    while True:
        eventlet.sleep(1)


class ExampleService(object):
    parallel = parallel_provider()

    @timer(interval=1)
    def handle_timer(self):
        with self.parallel(self) as parallel_self:
            parallel_self.keep_busy()

    def keep_busy(self):
        everlasting_call()


def test_parallel_executor_injection():
    config = Mock()
    container = ServiceContainer(ExampleService, WorkerContext, config)

    providers = list(get_injection_providers(container))
    assert len(providers) == 1
    provider = providers[0]

    assert provider.name == "parallel"
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
    sr.kill(Exception())
    with wait_for_call(5, kill_called) as kill_called_waited:
        kill_called_waited.assert_called_with(Exception)
