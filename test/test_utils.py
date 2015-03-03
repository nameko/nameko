# coding: utf-8

from eventlet import GreenPool, sleep
from eventlet.event import Event
import pytest

from nameko.utils import (
    fail_fast_imap, repr_safe_str, get_redacted_args, REDACTED)


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


@pytest.mark.parametrize("value, repr_safe_value", [
    ("bytestr", b"bytestr"),
    ("bÿtestr", b"b\xc3\xbftestr"),
    (u"unicode", b"unicode"),
    (u"unicøde", b"unic\xc3\xb8de"),
    (None, b"None"),  # cannot encode non-string
    (object, b"<type 'object'>"),  # cannot encode non-string
])
def test_repr_safe_str(value, repr_safe_value):
    res = repr_safe_str(value)
    assert res == repr_safe_value
    assert isinstance(res, bytes)


class TestGetRedactedArgs(object):

    @pytest.mark.parametrize("sensitive_variables, expected", [
        (tuple(), {'a': 'A', 'b': 'B'}),  # nothing redacted
        (("a",), {'a': REDACTED, 'b': 'B'}),  # only 'a' redacted
        (("a", "b"), {'a': REDACTED, 'b': REDACTED}),  # both refacted
        (("c"), {'a': 'A', 'b': 'B'}),  # 'c' not a valid argument; ignored
    ])
    def test_get_redacted_args(self, sensitive_variables, expected):

        class Service(object):
            def method(self, a, b):
                pass

        args = ("A", "B")
        kwargs = {}

        method = Service().method
        redacted = get_redacted_args(method, args, kwargs, sensitive_variables)
        assert redacted == expected

    @pytest.mark.parametrize("args, kwargs", [
        (('A', "B"), {}),  # all args
        (('A',), {'b': "B"}),
        (tuple(), {'a': "A", 'b': "B"}),  # all kwargs
    ])
    def test_get_redacted_args_invocation(self, args, kwargs):

        class Service(object):
            def method(self, a, b=None):
                pass

        sensitive_variables = ("a",)
        expected = {'a': REDACTED, 'b': 'B'}

        method = Service().method
        redacted = get_redacted_args(method, args, kwargs, sensitive_variables)
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
            ("a", "b.foo[0]", "b.foo[2]", "b.bar"),  # multiple keys
            {
                'a': REDACTED,
                'b': {'foo': [REDACTED, 2, REDACTED], 'bar': REDACTED},
            }
        ),
    ])
    def test_get_redacted_args_partial(self, sensitive_variables, expected):

        class Service(object):
            def method(self, a, b):
                pass

        complex_arg = {
            'foo': [1, 2, 3],
            'bar': "BAR"
        }

        args = ("A", complex_arg)
        kwargs = {}

        method = Service().method
        redacted = get_redacted_args(method, args, kwargs, sensitive_variables)
        assert redacted == expected
