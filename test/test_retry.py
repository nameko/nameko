import pytest
from mock import Mock, patch

from nameko.retry import retry


@pytest.fixture
def tracker():
    return Mock()


@pytest.yield_fixture(autouse=True)
def mock_sleep():
    with patch('nameko.retry.sleep') as patched:

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
def test_allowed_exceptions(exceptions, tracker):
    threshold = 10

    @retry(allowed_exceptions=exceptions, retries=float('inf'))
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
        retries = 5

        @retry(retries=retries)
        def fn():
            tracker()
            raise ValueError()

        with pytest.raises(ValueError):
            fn()

        assert tracker.call_count == 1 + retries

    def test_retry_forever(self, tracker):
        threshold = 10

        @retry(retries=None)
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

        retries = 5
        delay = 1

        @retry(retries=retries, delay=delay)
        def fn():
            tracker()
            raise ValueError()

        with pytest.raises(ValueError):
            fn()

        total_delay = mock_sleep.total()
        assert total_delay == delay * retries

    def test_backoff(self, tracker, mock_sleep):

        retries = 5
        delay = 2
        backoff = 3

        @retry(retries=retries, delay=delay, backoff=backoff)
        def fn():
            tracker()
            raise ValueError()

        with pytest.raises(ValueError):
            fn()

        total_delay = mock_sleep.total()
        assert total_delay == sum(
            delay * (backoff ** attempt) for attempt in range(1, retries + 1)
        )

    def test_max_delay(self, tracker, mock_sleep):

        retries = 5
        delay = 1
        backoff = 2
        max_delay = delay  # never increase delay, regardless of backoff

        @retry(
            retries=retries, delay=delay, backoff=backoff, max_delay=max_delay
        )
        def fn():
            tracker()
            raise ValueError()

        with pytest.raises(ValueError):
            fn()

        total_delay = mock_sleep.total()
        assert total_delay == delay * retries
