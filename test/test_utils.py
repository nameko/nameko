from eventlet import GreenPool, Timeout
from eventlet.event import Event
import pytest
from nameko.utils import fail_fast_imap


def test_fail_fast_imap():
    # A blocking call...
    blocking_event = Event()

    def blocking_call():
        blocking_event.wait()

    # ...a failing call...
    failing_exception = Exception()

    def failing_call():
        raise failing_exception

    # ...and a successful call.
    def good_call():
        return

    identity_fn = lambda x: x()
    calls = [good_call, blocking_call, failing_call]

    pool = GreenPool(3)

    # Show how normal imap call blocks
    with pytest.raises(Timeout):
        with Timeout(1):
            list(pool.imap(identity_fn, calls))

    # But fail_fast_imap fails as soon as the exception is raised
    with pytest.raises(Exception) as raised_exc:
        list(fail_fast_imap(pool, identity_fn, calls))
    assert raised_exc.value == failing_exception
