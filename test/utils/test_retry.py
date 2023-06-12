import pytest
from mock import Mock, patch

from nameko.utils.retry import retry


@pytest.fixture
def tracker():
    return Mock()


@pytest.fixture(autouse=True)
def mock_sleep():
    with patch('nameko.utils.retry.sleep') as patched:

        def total():
            return sum(delay for (delay,), _ in patched.call_args_list)

        patched.total = total
        yield patched


def test_without_arguments(tracker, mock_sleep):

    @retry
    def fn():
        tracker()
        raise ValueError()

    with pytest.raises(ValueError):
        fn()

    assert tracker.call_count > 1


@pytest.mark.parametrize("exceptions", [ValueError, (ValueError,)])
def test_retry_for_exceptions(exceptions, tracker):
    threshold = 10

    @retry(for_exceptions=exceptions, max_attempts=float('inf'))
    def fn():
        tracker()
        if tracker.call_count < threshold:
            raise ValueError()
        else:
            raise KeyError()

    with pytest.raises(KeyError):
        fn()

    assert tracker.call_count == threshold


class TestRetries(object):

    def test_retry_limit(self, tracker):
        max_attempts = 5

        @retry(max_attempts=max_attempts)
        def fn():
            tracker()
            raise ValueError()

        with pytest.raises(ValueError):
            fn()

        assert tracker.call_count == 1 + max_attempts

    def test_retry_forever(self, tracker):
        threshold = 10

        @retry(max_attempts=None)
        def fn():
            tracker()
            if tracker.call_count == threshold:
                return threshold
            else:
                raise ValueError()

        assert fn() == threshold
        assert tracker.call_count == threshold


class TestDelay(object):

    def test_fixed_delay(self, tracker, mock_sleep):

        max_attempts = 5
        delay = 1

        @retry(max_attempts=max_attempts, delay=delay)
        def fn():
            tracker()
            raise ValueError()

        with pytest.raises(ValueError):
            fn()

        total_delay = mock_sleep.total()
        assert total_delay == delay * max_attempts

    def test_backoff(self, tracker, mock_sleep):

        max_attempts = 5
        delay = 2
        backoff = 3

        @retry(max_attempts=max_attempts, delay=delay, backoff=backoff)
        def fn():
            tracker()
            raise ValueError()

        with pytest.raises(ValueError):
            fn()

        total_delay = mock_sleep.total()
        assert total_delay == sum(
            delay * (backoff ** attempt)
            for attempt in range(1, max_attempts + 1)
        )

    def test_max_delay(self, tracker, mock_sleep):

        max_attempts = 5
        delay = 1
        backoff = 2
        max_delay = delay  # never increase delay, regardless of backoff

        @retry(
            max_attempts=max_attempts, delay=delay,
            backoff=backoff, max_delay=max_delay
        )
        def fn():
            tracker()
            raise ValueError()

        with pytest.raises(ValueError):
            fn()

        total_delay = mock_sleep.total()
        assert total_delay == delay * max_attempts

    def test_multiple_calls(self, tracker, mock_sleep):
        # Make sure that delay behaves the same way if called multiple times.
        # Previously a bug caused the delay to increase with subsequent calls
        max_attempts = 2
        delay = 5
        backoff = 2

        @retry(max_attempts=max_attempts, delay=delay, backoff=backoff)
        def fn():
            tracker()
            raise ValueError()

        with pytest.raises(ValueError):
            fn()

        expected_delay = sum(
            delay * (backoff ** attempt)
            for attempt in range(1, max_attempts + 1)
        )
        total_delay = mock_sleep.total()
        assert total_delay == expected_delay

        with pytest.raises(ValueError):
            fn()

        total_delay = mock_sleep.total()
        assert total_delay == 2 * expected_delay
