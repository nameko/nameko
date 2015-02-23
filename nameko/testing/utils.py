"""
Common testing utilities.
"""
from contextlib import contextmanager
from functools import partial
from six.moves.urllib.parse import urlparse, quote

import eventlet
from mock import Mock
# from pyrabbit.http import HTTPError
import requests


from nameko.containers import WorkerContextBase
from nameko.extensions import Entrypoint


class Client2(object):
    def __init__(self, host_port, username, password):
        self._base_url = 'http://{}/api'.format(host_port)
        self._session = requests.Session()
        self._session.auth = (username, password)
        self._session.headers['content-type'] = 'application/json'

    @staticmethod
    def quote(value):
        return quote(value, '')

    def url(self, *args):
        args = map(self.quote, args)
        return '{}/{}'.format(
            self._base_url,
            '/'.join(args),
        )

    def get_connections(self):
        return self._session.get(self.url('connections')).json()

    def delete_connection(self, name):
        return self._session.delete(self.url('connections', name))

    def create_vhost(self, vhost):
        return self._session.put(self.url('vhosts', vhost))

    def delete_vhost(self, vhost):
        return self._session.delete(self.url('vhosts', vhost))

    def set_vhost_permissions(self, vhost, username, configure, read, write):
        permissions = {
            'configure': configure,
            'read': read,
            'write': write,
        }
        return self._session.put(
            self.url('permissions', vhost, username),
            json=permissions)


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

            def has_attribute(name, value):
                return getattr(ext, name) == value

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
def wait_for_call(timeout, mock_method):
    """ Return a context manager that waits ``timeout`` seconds for
    ``mock_method`` to be called, yielding the mock if so.

    Raises an :class:`eventlet.Timeout` if the method was not called
    within ``timeout`` seconds.
    """
    with eventlet.Timeout(timeout):
        while not mock_method.called:
            eventlet.sleep()
    yield mock_method


def wait_for_worker_idle(container, timeout=10):
    """ Blocks until ``container`` has no running workers.

    Raises an :class:`eventlet.Timeout` if the method was not called
    within ``timeout`` seconds.
    """
    with eventlet.Timeout(timeout):
        container._worker_pool.waitall()


def assert_stops_raising(fn, exception_type=Exception, timeout=10,
                         interval=0.1):
    """Assert that ``fn`` returns successfully within ``timeout``
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
    return Client2(host_port, uri.username, uri.password)


def reset_rabbit_vhost(vhost, username, rabbit_manager):

    try:
        rabbit_manager.delete_vhost(vhost)
    except Exception as exc:  # TODO
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
        except Exception as exc:  # TODO
            import pdb; pdb.set_trace()
            if exc.status == 404:
                pass  # connection closed in a race
            else:
                raise
