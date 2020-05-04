"""Test concurrency utilities."""

import pytest
from mock import patch

import nameko
from nameko.concurrency import (
    Event, Pool, fail_fast_imap, monkey_patch_if_enabled, sleep, yield_thread
)
from nameko.constants import MONKEY_PATCH_ENABLED_CONFIG_KEY


def test_fail_fast_imap():
    # A failing call...
    failing_exception = Exception()

    def failing_call():
        raise failing_exception

    # ...and an eventually successful call.
    slow_call_returned = Event()

    def slow_call():
        sleep(5)
        slow_call_returned.send()  # pragma: no cover

    def identity_fn(fn):
        return fn()

    calls = [slow_call, failing_call]

    pool = Pool(2)

    # fail_fast_imap fails as soon as the exception is raised
    with pytest.raises(Exception) as raised_exc:
        list(fail_fast_imap(pool, identity_fn, calls))
    assert raised_exc.value == failing_exception

    # The slow call won't go past the sleep as it was killed
    assert not slow_call_returned.ready()
    yield_thread()
    assert pool.free() == 2


@patch('nameko.concurrency.monkey_patch')
@pytest.mark.parametrize('config_patch,expected_call_count', [
    ({MONKEY_PATCH_ENABLED_CONFIG_KEY: True}, 1),
    ({MONKEY_PATCH_ENABLED_CONFIG_KEY: False}, 0),
    ({}, 1)
])
def test_monkey_patch_if_enabled(patched_monkey, config_patch, expected_call_count):
    with nameko.config.patch(config_patch):
        monkey_patch_if_enabled()
        assert patched_monkey.call_count == expected_call_count
