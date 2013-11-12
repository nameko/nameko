from mock import Mock
from nameko.parallel import ParallelExecutor


def test_parallel_executor_gives_submit():
    fn = Mock()
    ParallelExecutor.submit(fn, 1)
    fn.assert_called_with(1)


def test_parallel_wrapped_calls():
    to_wrap = Mock()
    to_wrap.foo = 3
    fn = Mock()
    to_wrap.wrapped_call = fn
    assert ParallelExecutor(to_wrap).foo == 3
    ParallelExecutor(to_wrap).wrapped_call(2)
    fn.assert_called_with(2)
