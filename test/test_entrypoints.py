from mock import Mock, call
import pytest

from nameko.testing.services import once


method_called = Mock()


@pytest.yield_fixture(autouse=True)
def reset():
    yield
    method_called.reset_mock()


def test_decorator_without_args(container_factory):

    class Service(object):
        name = "service"

        @once
        def method(self, a="a", b="b"):
            method_called(a, b)

    container = container_factory(Service, config={})
    container.start()
    container.stop()  # graceful, waits for workers

    assert method_called.call_args == call('a', 'b')


def test_decorator_with_args(container_factory):

    class Service(object):
        name = "service"

        @once("x")
        def method(self, a, b="b"):
            method_called(a, b)

    container = container_factory(Service, config={})
    container.start()
    container.stop()  # graceful, waits for workers

    assert method_called.call_args == call('x', 'b')


def test_decorator_with_kwargs(container_factory):

    class Service(object):
        name = "service"

        @once(b="x")
        def method(self, a="a", b="b"):
            method_called(a, b)

    container = container_factory(Service, config={})
    container.start()
    container.stop()  # graceful, waits for workers

    assert method_called.call_args == call('a', 'x')
