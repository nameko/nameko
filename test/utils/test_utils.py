# coding: utf-8

import pytest
from eventlet import GreenPool, sleep
from eventlet.event import Event

import nameko.rpc
from nameko.containers import ServiceContainer
from nameko.extensions import DependencyProvider
from nameko.rpc import Rpc, rpc
from nameko.testing.services import dummy, entrypoint_hook, get_extension
from nameko.utils import (
    REDACTED, fail_fast_imap, get_redacted_args, import_from_path
)


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

    pool = GreenPool(2)

    # fail_fast_imap fails as soon as the exception is raised
    with pytest.raises(Exception) as raised_exc:
        list(fail_fast_imap(pool, identity_fn, calls))
    assert raised_exc.value == failing_exception

    # The slow call won't go past the sleep as it was killed
    assert not slow_call_returned.ready()
    assert pool.free() == 2


class TestGetRedactedArgs(object):

    @pytest.mark.parametrize("sensitive_arguments, expected", [
        (tuple(), {'a': 'A', 'b': 'B'}),  # no sensitive arguments
        ("a", {'a': REDACTED, 'b': 'B'}),
        (("a",), {'a': REDACTED, 'b': 'B'}),
        (("a", "b"), {'a': REDACTED, 'b': REDACTED}),
        (("c"), {'a': 'A', 'b': 'B'}),  # 'c' not a valid argument; ignored
    ])
    def test_get_redacted_args(
        self, sensitive_arguments, expected, rabbit_config
    ):

        class Service(object):
            name = "service"

            @rpc(sensitive_arguments=sensitive_arguments)
            def method(self, a, b):
                pass  # pragma: no cover

        args = ("A", "B")
        kwargs = {}

        container = ServiceContainer(Service, rabbit_config)
        entrypoint = get_extension(container, Rpc)

        redacted = get_redacted_args(entrypoint, *args, **kwargs)
        assert redacted == expected

    @pytest.mark.parametrize("args, kwargs", [
        (('A', "B"), {}),  # all args
        (('A',), {'b': "B"}),
        (tuple(), {'a': "A", 'b': "B"}),  # all kwargs
    ])
    def test_get_redacted_args_invocation(self, args, kwargs, rabbit_config):

        class Service(object):
            name = "service"

            @rpc(sensitive_arguments="a")
            def method(self, a, b=None):
                pass  # pragma: no cover

        expected = {'a': REDACTED, 'b': 'B'}

        container = ServiceContainer(Service, rabbit_config)
        entrypoint = get_extension(container, Rpc)

        redacted = get_redacted_args(entrypoint, *args, **kwargs)
        assert redacted == expected

    @pytest.mark.parametrize("sensitive_arguments, expected", [
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
    def test_get_redacted_args_partial(
        self, sensitive_arguments, expected, rabbit_config
    ):

        class Service(object):
            name = "service"

            @rpc(sensitive_arguments=sensitive_arguments)
            def method(self, a, b):
                pass  # pragma: no cover

        complex_arg = {
            'foo': [1, 2, 3],
            'bar': "BAR"
        }

        args = ("A", complex_arg)
        kwargs = {}

        container = ServiceContainer(Service, rabbit_config)
        entrypoint = get_extension(container, Rpc)

        redacted = get_redacted_args(entrypoint, *args, **kwargs)
        assert redacted == expected

    def test_get_redacted_args_partial_no_mutation(self, container_factory):
        """ Calling `get_redacted_args` should not mutate anything received by
        the entrypoint.
        """
        redacted = {}

        class Redactor(DependencyProvider):
            """ Example DependencyProvider that redacts `sensitive_arguments`
            on entrypoints during the worker lifecycle.
            """

            def worker_setup(self, worker_ctx):
                entrypoint = worker_ctx.entrypoint
                args = worker_ctx.args
                kwargs = worker_ctx.kwargs

                redacted.update(get_redacted_args(entrypoint, *args, **kwargs))

        class Service(object):
            name = "service"

            redactor = Redactor()

            @dummy(sensitive_arguments=['b[foo][1]'])
            def method(self, a, b):
                return {
                    "a": a,
                    "b": b
                }

        container = container_factory(Service, {})

        complex_arg = {
            'foo': [1, 2, 3],
            'bar': "BAR"
        }

        with entrypoint_hook(container, 'method') as hook:
            result = hook("A", complex_arg)

        assert redacted == {
            'a': 'A',
            'b': {
                'foo': [1, REDACTED, 3],
                'bar': "BAR"
            }
        }
        assert result == {
            'a': 'A',
            'b': {
                'foo': [1, 2, 3],
                'bar': "BAR"
            }
        }


class TestImportFromPath(object):

    def test_path_is_none(self):
        assert import_from_path(None) is None

    def test_import_error(self):
        with pytest.raises(ImportError) as exc_info:
            import_from_path("foo.bar.Baz")
        assert (
            "`foo.bar.Baz` could not be imported" in str(exc_info.value)
        )

    def test_import_class(self):
        assert import_from_path("nameko.rpc.Rpc") is Rpc

    def test_import_module(self):
        assert import_from_path("nameko.rpc") is nameko.rpc

    def test_import_function(self):
        assert import_from_path("nameko.rpc.rpc") is rpc
