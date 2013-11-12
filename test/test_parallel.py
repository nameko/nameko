from mock import Mock
from nameko.parallel import ParallelExecutor


def test_parallel_executor_gives_submit():
    fn = Mock()
    ParallelExecutor.submit(fn, 1)
    fn.assert_called_with(1)
