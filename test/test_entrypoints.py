from collections import defaultdict

import pytest
from mock import Mock, call

from nameko.extensions import DependencyProvider, Entrypoint
from nameko.testing.services import dummy, entrypoint_hook, once
from nameko.testing.utils import get_extension
from nameko.utils import REDACTED, get_redacted_args


@pytest.fixture
def tracker():
    return Mock()


class TestDecorator(object):

    def test_decorator_without_args(self, container_factory, tracker):

        class Service(object):
            name = "service"

            @once
            def method(self, a="a", b="b"):
                tracker(a, b)

        container = container_factory(Service)
        container.start()
        container.stop()  # graceful, waits for workers

        assert tracker.call_args == call('a', 'b')

    def test_decorator_with_args(self, container_factory, tracker):

        class Service(object):
            name = "service"

            @once("x")
            def method(self, a, b="b"):
                tracker(a, b)

        container = container_factory(Service)
        container.start()
        container.stop()  # graceful, waits for workers

        assert tracker.call_args == call('x', 'b')

    def test_decorator_with_kwargs(self, container_factory, tracker):

        class Service(object):
            name = "service"

            @once(b="x")
            def method(self, a="a", b="b"):
                tracker(a, b)

        container = container_factory(Service)
        container.start()
        container.stop()  # graceful, waits for workers

        assert tracker.call_args == call('a', 'x')


class TestExpectedExceptions(object):

    def test_expected_exceptions(self, container_factory):

        exceptions = defaultdict(list)

        class CustomException(Exception):
            pass

        class Logger(DependencyProvider):
            """ Example DependencyProvider that interprets
            ``expected_exceptions`` on an entrypoint
            """

            def worker_result(self, worker_ctx, result=None, exc_info=None):
                if exc_info is None:  # nothing to do
                    return  # pragma: no cover

                exc = exc_info[1]
                expected = worker_ctx.entrypoint.expected_exceptions

                if isinstance(exc, expected):
                    exceptions['expected'].append(exc)
                else:
                    exceptions['unexpected'].append(exc)

        class Service(object):
            name = "service"

            logger = Logger()

            @dummy(expected_exceptions=CustomException)
            def expected(self):
                raise CustomException()

            @dummy
            def unexpected(self):
                raise CustomException()

        container = container_factory(Service)
        container.start()

        with entrypoint_hook(container, 'expected') as hook:
            with pytest.raises(CustomException) as expected_exc:
                hook()
        assert expected_exc.value in exceptions['expected']

        with entrypoint_hook(container, 'unexpected') as hook:
            with pytest.raises(CustomException) as unexpected_exc:
                hook()
        assert unexpected_exc.value in exceptions['unexpected']


class TestSensitiveArguments(object):

    def test_sensitive_arguments(self, container_factory):

        redacted = {}

        class Logger(DependencyProvider):
            """ Example DependencyProvider that makes use of
            ``get_redacted_args`` to redact ``sensitive_arguments``
            on entrypoints.
            """

            def worker_setup(self, worker_ctx):
                entrypoint = worker_ctx.entrypoint
                args = worker_ctx.args
                kwargs = worker_ctx.kwargs

                redacted.update(get_redacted_args(entrypoint, *args, **kwargs))

        class Service(object):
            name = "service"

            logger = Logger()

            @dummy(sensitive_arguments=("a", "b.x[0]", "b.x[2]"))
            def method(self, a, b, c):
                return [a, b, c]

        container = container_factory(Service)
        entrypoint = get_extension(container, Entrypoint)

        assert entrypoint.sensitive_arguments == ("a", "b.x[0]", "b.x[2]")

        a = "A"
        b = {'x': [1, 2, 3], 'y': [4, 5, 6]}
        c = "C"

        with entrypoint_hook(container, "method") as method:
            assert method(a, b, c) == [a, b, c]

        assert redacted == {
            'a': REDACTED,
            'b': {
                'x': [REDACTED, 2, REDACTED],
                'y': [4, 5, 6]
            },
            'c': 'C'
        }
