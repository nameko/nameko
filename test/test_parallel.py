from mock import Mock
from nameko.parallel import ParallelExecutor
from nameko.service import ManagedThreadContainer
from nameko.testing.utils import wait_for_call


def test_parallel_executor_gives_submit():
    to_call = Mock()
    ParallelExecutor(ManagedThreadContainer()).submit(to_call, 1)
    with wait_for_call(5, to_call) as to_call_waited:
        to_call_waited.assert_called_with(1)


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
