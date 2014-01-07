"""
Common testing utilities.
"""
from contextlib import contextmanager
from functools import partial

import eventlet
from mock import Mock

from nameko.containers import WorkerContextBase


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


def get_container(runner, service_cls):
    """ Inspect ``runner.containers`` and return the first item that is
    hosting an instance of ``service_cls``.
    """
    for container in runner.containers:
        if container.service_cls == service_cls:
            return container


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


def assert_stops_raising(fn, exception_type=Exception, timeout=10,
                         interval=0.1):
    """Assert that ``fn`` returns succesfully within ``timeout``
       seconds, trying every ``interval`` seconds.

       If ``exception_type`` is provided, fail unless the exception thrown is
       an instance of ``exception_type``. If not specified, any
       `:class:`Exception` instance is allowed.
    """
    with eventlet.Timeout(timeout):
        while True:
            try:
                fn()
            except exception_type:
                pass
            else:
                return
            eventlet.sleep(interval)


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


def worker_context_factory(*keys):
    class CustomWorkerContext(WorkerContextBase):
        context_keys = keys

        def __init__(self, container=None, service=None, method_name=None,
                     **kwargs):
            container_mock = Mock()
            container_mock.config = {}
            super(CustomWorkerContext, self).__init__(
                container or container_mock,
                service or Mock(),
                method_name or Mock(),
                **kwargs
            )

    return CustomWorkerContext
