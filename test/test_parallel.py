from mock import Mock
from nameko.parallel import ParallelExecutor


class SimpleThreadProvider(object):
    def spawn_managed_thread(self, run_method):
        # Runs synchronously, doesn't use a thread
        return run_method()


def test_parallel_executor_gives_submit():
    to_call = Mock()
    ParallelExecutor(SimpleThreadProvider()).submit(to_call, 1)
    to_call.assert_called_with(1)


def test_parallel_wrapper_layer():
    to_wrap = Mock()
    to_wrap.wrapped_attribute = 2
    to_call = Mock()
    to_wrap.wrapped_call = to_call
    executor = ParallelExecutor(SimpleThreadProvider())

    # Confirm wrapped call comes via submit
    orig_submit = executor.submit

    submit_check = Mock()

    def pass_through_submit(*args, **kwargs):
        submit_check()
        orig_submit(*args, **kwargs)

    executor.submit = pass_through_submit

    assert executor(to_wrap).wrapped_attribute == 2
    executor(to_wrap).wrapped_call(3)
    to_call.assert_called_with(3)
    submit_check.assert_called_with()


def test_parallel_executor_context_manager():
    to_call = Mock()
    with ParallelExecutor(SimpleThreadProvider()) as execution_context:
        execution_context.submit(to_call, 4)
    to_call.assert_called_with(4)


def test_parallel_executor_wrapped_cm():
    to_wrap = Mock()
    to_call = Mock()
    to_wrap.wrapped_call = to_call
    with ParallelExecutor(SimpleThreadProvider())(to_wrap) as wrapped:
        wrapped.wrapped_call(5)
    to_call.assert_called_with(5)
