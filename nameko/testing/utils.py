"""
Common testing utilities.
"""
import socket
import warnings
from collections import namedtuple
from contextlib import contextmanager
from functools import partial

import eventlet

from nameko.extensions import Entrypoint
from nameko.testing.rabbit import HTTPError


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
    warnings.warn(
        "`wait_for_worker_idle` is deprecated. Use the `entrypoint_waiter` "
        "to wait for specific entrypoints instead.", DeprecationWarning
    )
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


MockCallArgs = namedtuple("MockCallArgs", ["positional", "keyword"])


def unpack_mock_call(call):
    return MockCallArgs(*call)


class AnyInstanceOf(object):

    def __init__(self, cls):
        self.cls = cls

    def __eq__(self, other):
        return isinstance(self.cls, type) and isinstance(other, self.cls)

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        obj = getattr(self.cls, '__name__', self.cls)
        return '<AnyInstanceOf-{}>'.format(obj)


ANY_PARTIAL = AnyInstanceOf(partial)


class DummyProvider(Entrypoint):
    def __init__(self, method_name=None):
        self.method_name = method_name


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
            if exc.response.status_code == 404:
                pass  # connection closed in a race
            else:
                raise


def find_free_port(host='127.0.0.1'):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((host, 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


class ResourcePipeline(object):
    """ Creates and destroys resources in background threads.

    Creates up to `size` resources ahead of time so the caller avoids waiting
    for lazy creation.
    """

    STOP = object()

    def __init__(self, create, destroy, size=3):
        if size == 0:
            raise RuntimeError("Zero size would create unbounded resources")
        self.ready = eventlet.Queue(maxsize=size)
        self.trash = eventlet.Queue()
        self.size = size

        self.create = create
        self.destroy = destroy

    def _start(self):
        self.running = True
        self.create_thread = eventlet.spawn(self._create)
        self.destroy_thread = eventlet.spawn(self._destroy)

    def _shutdown(self):
        self.running = False

        # increase max size of the ready queue and yield, allowing the
        # create thread to exit now if it's blocked trying to put an item
        self.ready.resize(self.size + 1)
        eventlet.sleep()

        # trash unused items while there are any left in the queue,
        # or the create thread is still running
        while self.ready.qsize() or not self.create_thread.dead:
            unused = self.ready.get()
            self.trash.put(unused)

        # finally wait for the destroy thread to exit
        self.trash.put(ResourcePipeline.STOP)
        self.destroy_thread.wait()

    def _create(self):
        while self.running:
            item = self.create()
            self.ready.put(item)

    def _destroy(self):
        while True:
            item = self.trash.get()
            if item is ResourcePipeline.STOP:
                break
            self.destroy(item)

    @contextmanager
    def get(self):
        item = self.ready.get()
        try:
            yield item
        finally:
            self.trash.put(item)

    @contextmanager
    def run(self):
        self._start()
        try:
            yield self
        finally:
            self._shutdown()
