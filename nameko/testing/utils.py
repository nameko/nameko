import eventlet
from contextlib import contextmanager

from functools import partial


def get_dependency(container, dependency_cls, **match_attrs):
    """ Inspect ``container.dependencies`` and return the first item that is
    an instance of ``dependency_cls``.

    Optionally also require that the instance has an attribute with a
    particular value as given in the ``match_attrs`` kwargs.
    """
    for dep in container.dependencies:
        if isinstance(dep, dependency_cls):
            if not match_attrs:
                return dep

            has_attribute = lambda name, value: getattr(dep, name) == value
            if all([has_attribute(name, value)
                    for name, value in match_attrs.items()]):
                return dep


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


class AnyInstanceOf(object):

    def __init__(self, cls):
        self.cls = cls

    def __eq__(self, other):
        return isinstance(other, self.cls)

    def __ne__(self, other):
        try:
            return not isinstance(other, self.cls)
        except TypeError:
            return True

    def __repr__(self):
        obj = getattr(self.cls, '__name__', self.cls)
        return '<AnyInstanceOf-{}>'.format(obj)


ANY_PARTIAL = AnyInstanceOf(partial)
