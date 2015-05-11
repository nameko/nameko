# coding: utf-8

from eventlet import GreenPool, sleep
from eventlet.event import Event
import pytest

from nameko.containers import ServiceContainer
from nameko.rpc import rpc, Rpc
from nameko.utils import fail_fast_imap, get_redacted_args, REDACTED
from nameko.testing.services import get_extension


def test_fail_fast_imap():
    # A failing call...
    failing_exception = Exception()

    def failing_call():
        raise failing_exception

    # ...and an eventually successful call.
    slow_call_returned = Event()

    def slow_call():
        sleep(5)
        slow_call_returned.send()

    def identity_fn(fn):
        return fn()

    calls = [slow_call, failing_call]

    pool = GreenPool(2)

    # fail_fast_imap fails as soon as the exception is raised
    with pytest.raises(Exception) as raised_exc:
        list(fail_fast_imap(pool, identity_fn, calls))
    assert raised_exc.value == failing_exception

    # The slow call won't go past the sleep as it was killed
    assert not slow_call_returned.ready()
    assert pool.free() == 2


class TestGetRedactedArgs(object):

    @pytest.mark.parametrize("sensitive_variables, expected", [
        (tuple(), {'a': 'A', 'b': 'B'}),  # no sensitive variables
        ("a", {'a': REDACTED, 'b': 'B'}),
        (("a",), {'a': REDACTED, 'b': 'B'}),
        (("a", "b"), {'a': REDACTED, 'b': REDACTED}),
        (("c"), {'a': 'A', 'b': 'B'}),  # 'c' not a valid argument; ignored
    ])
    def test_get_redacted_args(self, sensitive_variables, expected):

        class Service(object):
            name = "service"

            @rpc(sensitive_variables=sensitive_variables)
            def method(self, a, b):
                pass

        args = ("A", "B")
        kwargs = {}

        container = ServiceContainer(Service, {})
        entrypoint = get_extension(container, Rpc)

        redacted = get_redacted_args(entrypoint, *args, **kwargs)
        assert redacted == expected

    @pytest.mark.parametrize("args, kwargs", [
        (('A', "B"), {}),  # all args
        (('A',), {'b': "B"}),
        (tuple(), {'a': "A", 'b': "B"}),  # all kwargs
    ])
    def test_get_redacted_args_invocation(self, args, kwargs):

        class Service(object):
            name = "service"

            @rpc(sensitive_variables="a")
            def method(self, a, b=None):
                pass

        expected = {'a': REDACTED, 'b': 'B'}

        container = ServiceContainer(Service, {})
        entrypoint = get_extension(container, Rpc)

        redacted = get_redacted_args(entrypoint, *args, **kwargs)
        assert redacted == expected

    @pytest.mark.parametrize("sensitive_variables, expected", [
        (
            ("b.foo",),  # dict key
            {
                'a': "A",
                'b': {'foo': REDACTED, 'bar': "BAR"},
            },
        ),
        (
            ("b.foo[0]",),   # list index
            {
                'a': "A",
                'b': {'foo': [REDACTED, 2, 3], 'bar': "BAR"},
            }
        ),
        (
            ("b.not_a_key[0]",),  # missing keys ignored
            {
                'a': "A",
                'b': {'foo': [1, 2, 3], 'bar': "BAR"},
            }
        ),
        (
            ("b.foo[999]",),   # missing indices ignored
            {
                'a': "A",
                'b': {'foo': [1, 2, 3], 'bar': "BAR"},
            }
        ),
        (
            ("a.not_a_dictionary",),   # incorrect partials ignored
            {
                'a': "A",
                'b': {'foo': [1, 2, 3], 'bar': "BAR"},
            }
        ),
        (
            ("a", "b.foo[0]", "b.foo[2]", "b.bar"),  # multiple keys
            {
                'a': REDACTED,
                'b': {'foo': [REDACTED, 2, REDACTED], 'bar': REDACTED},
            }
        ),
    ])
    def test_get_redacted_args_partial(self, sensitive_variables, expected):

        class Service(object):
            name = "service"

            @rpc(sensitive_variables=sensitive_variables)
            def method(self, a, b):
                pass

        complex_arg = {
            'foo': [1, 2, 3],
            'bar': "BAR"
        }

        args = ("A", complex_arg)
        kwargs = {}

        container = ServiceContainer(Service, {})
        entrypoint = get_extension(container, Rpc)

        redacted = get_redacted_args(entrypoint, *args, **kwargs)
        assert redacted == expected
