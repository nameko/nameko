import eventlet
from contextlib import contextmanager

from mock import PropertyMock
from functools import partial


@contextmanager
def wait_for_call(timeout, mock_method):
    """ Return a context manager that waits ``timeout`` seconds for
    ``mock_method`` to be called, yielding the mock if so.

    Raises an eventlet.TimeoutError if the method was not called within
    ``timeout``.
    """
    with eventlet.Timeout(timeout):
        while not mock_method.called:
            eventlet.sleep()
    yield mock_method


@contextmanager
def as_context_manager(obj):
    """ Return a context manager that provides ``obj`` on enter.
    """
    yield obj


def as_mock_property(obj):
    """ Return a PropertyMock that returns ``obj`` as its value.
    """
    mock = PropertyMock()
    mock.return_value = obj
    return mock


class AnyInstanceOf(object):

    def __init__(self, cls):
        self.cls = cls

    def __eq__(self, other):
        return isinstance(other, self.cls)

    def __ne__(self, other):
        return not isinstance(other, self.cls)

    def __repr__(self):
        return '<AnyInstanceOf-{}>'.format(self.cls)


ANY_PARTIAL = AnyInstanceOf(partial)
