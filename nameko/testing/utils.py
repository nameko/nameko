"""
Common testing utilities.
"""
from contextlib import contextmanager
from functools import partial
from urlparse import urlparse

import eventlet
from mock import Mock, patch
from pyrabbit.api import Client
from pyrabbit.http import HTTPError


from nameko.containers import WorkerContextBase
from nameko.extensions import Entrypoint


def get_extension(container, extension_cls, **match_attrs):
    """ Inspect ``container.extensions`` and return the first item that is
    an instance of ``extension_cls``.

    Optionally also require that the instance has an attribute with a
    particular value as given in the ``match_attrs`` kwargs.
    """
    for ext in container.extensions:
        if isinstance(ext, extension_cls):
            if not match_attrs:
                return ext

            has_attribute = lambda name, value: getattr(ext, name) == value
            if all([has_attribute(name, value)
                    for name, value in match_attrs.items()]):
                return ext


def get_container(runner, service_cls):
    """ Inspect ``runner.containers`` and return the first item that is
    hosting an instance of ``service_cls``.
    """
    for container in runner.containers:
        if container.service_cls == service_cls:
            return container


@contextmanager
def mock_extension(extension_cls):
    """ Ensure that new bound instances of `extension_cls` are Mock objects,
    intended to be used inside a test fixture.

    This is wrapped up into a helper because the mock objects must be reset
    after fixture teardown, or they keep strong references to any
    ServiceContainer used in the test, which leads to memory leaks.
    """
    ext_mock = Mock(spec=extension_cls)
    with patch.object(extension_cls, 'bind') as bind:
        bind.return_value = ext_mock
        yield ext_mock

    # reset mocks to clear references to `container`
    ext_mock.reset_mock()
    bind.reset_mock()


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


def wait_for_worker_idle(container, timeout=10):
    """ Blocks until ``container`` has no running workers.

    Raises an eventlet.TimeoutError if the workers did not complete within
    ``timeout`` seconds.
    """
    with eventlet.Timeout(timeout):
        container._worker_pool.waitall()


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


class DummyProvider(Entrypoint):
    def __init__(self, method_name=None):
        self.method_name = method_name


def worker_context_factory(*keys):
    class CustomWorkerContext(WorkerContextBase):
        context_keys = keys

        def __init__(self, container=None, service=None, entrypoint=None,
                     **kwargs):
            container_mock = Mock()
            container_mock.config = {}
            super(CustomWorkerContext, self).__init__(
                container or container_mock,
                service or Mock(),
                entrypoint or Mock(),
                **kwargs
            )

    return CustomWorkerContext


def get_rabbit_config(amqp_uri):
    conf = {'AMQP_URI': amqp_uri}

    uri = urlparse(amqp_uri)
    conf['vhost'] = uri.path[1:]
    conf['username'] = uri.username

    return conf


def get_rabbit_manager(rabbit_ctl_uri):
    uri = urlparse(rabbit_ctl_uri)
    host_port = '{0.hostname}:{0.port}'.format(uri)
    return Client(host_port, uri.username, uri.password)


def reset_rabbit_vhost(vhost, username, rabbit_manager):

    try:
        rabbit_manager.delete_vhost(vhost)
    except HTTPError as exc:
        if exc.status == 404:
            pass  # vhost does not exist
        else:
            raise
    rabbit_manager.create_vhost(vhost)
    rabbit_manager.set_vhost_permissions(vhost, username, '.*', '.*', '.*')


def get_rabbit_connections(vhost, rabbit_manager):

    connections = rabbit_manager.get_connections()
    if connections is not None:
        return [connection for connection in connections
                if connection['vhost'] == vhost]
    return []


def reset_rabbit_connections(vhost, rabbit_manager):

    for connection in get_rabbit_connections(vhost, rabbit_manager):
        try:
            rabbit_manager.delete_connection(connection['name'])
        except HTTPError as exc:
            if exc.status == 404:
                pass  # connection closed in a race
            else:
                raise
