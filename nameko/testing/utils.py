import inspect
from contextlib import contextmanager
from functools import partial

import eventlet
from mock import Mock, patch
from nameko.containers import WorkerContextBase

from nameko.dependencies import DependencyFactory, InjectionProvider


def instance_factory(service_cls, **injections):
    """ Return an instance of ``service_cls`` with its injected dependencies
    replaced with Mock objects, or as given in ``injections``.
    """
    service = service_cls()
    for name, attr in inspect.getmembers(service):
        if isinstance(attr, DependencyFactory):
            factory = attr
            if issubclass(factory.dep_cls, InjectionProvider):
                try:
                    injection = injections[name]
                except KeyError:
                    injection = Mock()
                setattr(service, name, injection)
    return service


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


class Increment(object):
    def __init__(self):
        self.i = -1

    def next(self):
        self.i += 1
        return str(self.i)


@contextmanager
def predictable_call_ids():
    with patch('nameko.containers.new_call_id') as get_id:
        i = Increment()
        get_id.side_effect = i.next
        yield get_id


def worker_context_factory(*keys):
    class NewContext(WorkerContextBase):
        data_keys = keys

        def __init__(self, container=None, service=None, method_name=None,
                     **kwargs):
            super(NewContext, self).__init__(
                container or Mock(), service or Mock(), method_name or Mock(),
                **kwargs
            )

    return NewContext
