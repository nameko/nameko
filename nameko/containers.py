from __future__ import absolute_import, unicode_literals

import inspect
import sys
import uuid
import warnings
from collections import deque
from logging import getLogger

import eventlet
import six
from eventlet.event import Event
from eventlet.greenpool import GreenPool
from greenlet import GreenletExit  # pylint: disable=E0611

from nameko import config, serialization
from nameko.constants import (
    CALL_ID_STACK_CONTEXT_KEY, DEFAULT_MAX_WORKERS,
    DEFAULT_PARENT_CALLS_TRACKED, MAX_WORKERS_CONFIG_KEY,
    PARENT_CALLS_CONFIG_KEY
)
from nameko.exceptions import ConfigurationError, ContainerBeingKilled
from nameko.extensions import (
    ENTRYPOINT_EXTENSIONS_ATTR, is_dependency, iter_extensions
)
from nameko.log_helpers import make_timing_logger
from nameko.utils import import_from_path
from nameko.utils.concurrency import SpawningSet


_log = getLogger(__name__)
_log_time = make_timing_logger(_log)

if six.PY2:  # pragma: no cover
    is_method = inspect.ismethod
else:  # pragma: no cover
    is_method = inspect.isfunction


def get_service_name(service_cls):
    service_name = getattr(service_cls, 'name', None)
    if service_name is None:
        raise ConfigurationError(
            'Service class must define a `name` attribute ({}.{})'.format(
                service_cls.__module__, service_cls.__name__))
    if not isinstance(service_name, six.string_types):
        raise ConfigurationError(
            'Service name attribute must be a string ({}.{}.name)'.format(
                service_cls.__module__, service_cls.__name__))
    return service_name


def get_container_cls():
    class_path = config.get('SERVICE_CONTAINER_CLS')
    return import_from_path(class_path) or ServiceContainer


def new_call_id():
    return str(uuid.uuid4())


class WorkerContext(object):

    _parent_call_id_stack = None

    def __init__(
        self, container, service, entrypoint, args=None, kwargs=None, data=None
    ):
        self.container = container

        self.service = service
        self.entrypoint = entrypoint
        self.service_name = self.container.service_name

        self.args = args if args is not None else ()
        self.kwargs = kwargs if kwargs is not None else {}
        self.data = data if data is not None else {}

        self._parent_call_id_stack = self.data.pop(
            CALL_ID_STACK_CONTEXT_KEY, []
        )
        self.call_id = self._make_call_id()
        self.call_id_stack = self._make_call_id_stack()
        self.data[CALL_ID_STACK_CONTEXT_KEY] = self.call_id_stack

        self.context_data = self.data  # backwards compat

    def _make_call_id_stack(self):
        parent_calls_tracked = config.get(
            PARENT_CALLS_CONFIG_KEY, DEFAULT_PARENT_CALLS_TRACKED
        )
        stack_length = parent_calls_tracked + 1

        call_id_stack = deque(maxlen=stack_length)
        call_id_stack.extend(self._parent_call_id_stack)
        call_id_stack.append(self.call_id)
        return list(call_id_stack)

    def _make_call_id(self):
        return '{}.{}.{}'.format(
            self.service_name, self.entrypoint.method_name, new_call_id()
        )

    @property
    def origin_call_id(self):
        if self._parent_call_id_stack:
            return self._parent_call_id_stack[0]

    @property
    def immediate_parent_call_id(self):
        if self._parent_call_id_stack:
            return self._parent_call_id_stack[-1]

    def __repr__(self):
        cls_name = type(self).__name__
        service_name = self.service_name
        method_name = self.entrypoint.method_name
        return '<{} [{}.{}] at 0x{:x}>'.format(
            cls_name, service_name, method_name, id(self))


class ServiceContainer(object):

    def __init__(self, service_cls):

        self.service_cls = service_cls

        self.service_name = get_service_name(service_cls)
        self.shared_extensions = {}

        self.max_workers = (
            config.get(MAX_WORKERS_CONFIG_KEY) or DEFAULT_MAX_WORKERS)

        self.serializer, self.accept = serialization.setup()

        self.entrypoints = SpawningSet()
        self.dependencies = SpawningSet()
        self.subextensions = SpawningSet()

        for attr_name, dependency in inspect.getmembers(service_cls,
                                                        is_dependency):
            bound = dependency.bind(self.interface, attr_name)
            self.dependencies.add(bound)
            self.subextensions.update(iter_extensions(bound))

        for method_name, method in inspect.getmembers(service_cls, is_method):
            entrypoints = getattr(method, ENTRYPOINT_EXTENSIONS_ATTR, [])
            for entrypoint in entrypoints:
                bound = entrypoint.bind(self.interface, method_name)
                self.entrypoints.add(bound)
                self.subextensions.update(iter_extensions(bound))

        self.started = False
        self._worker_pool = GreenPool(size=self.max_workers)

        self._worker_threads = {}
        self._managed_threads = {}
        self._being_killed = False
        self._died = Event()

    @property
    def config(self):
        warnings.warn("Use ``nameko.config`` instead.", DeprecationWarning)
        return config

    @property
    def extensions(self):
        return SpawningSet(
            self.entrypoints | self.dependencies | self.subextensions
        )

    @property
    def interface(self):
        """ An interface to this container for use by extensions.
        """
        return self

    def start(self):
        """ Start a container by starting all of its extensions.
        """
        _log.debug('starting %s', self)
        self.started = True

        with _log_time('started %s', self):
            self.extensions.all.setup()
            self.extensions.all.start()

    def stop(self):
        """ Stop the container gracefully.

        First all entrypoints are asked to ``stop()``.
        This ensures that no new worker threads are started.

        It is the extensions' responsibility to gracefully shut down when
        ``stop()`` is called on them and only return when they have stopped.

        After all entrypoints have stopped the container waits for any
        active workers to complete.

        After all active workers have stopped the container stops all
        dependency providers.

        At this point there should be no more managed threads. In case there
        are any managed threads, they are killed by the container.
        """
        if self._died.ready():
            _log.debug('already stopped %s', self)
            return

        if self._being_killed:
            # this race condition can happen when a container is hosted by a
            # runner and yields during its kill method; if it's unlucky in
            # scheduling the runner will try to stop() it before self._died
            # has a result
            _log.debug('already being killed %s', self)
            try:
                self._died.wait()
            except:
                pass  # don't re-raise if we died with an exception
            return

        _log.debug('stopping %s', self)

        with _log_time('stopped %s', self):

            # entrypoint have to be stopped before dependencies to ensure
            # that running workers can successfully complete
            self.entrypoints.all.stop()

            # there might still be some running workers, which we have to
            # wait for to complete before we can stop dependencies
            self._worker_pool.waitall()

            # it should be safe now to stop any dependency as there is no
            # active worker which could be using it
            self.dependencies.all.stop()

            # finally, stop remaining extensions
            self.subextensions.all.stop()

            # any any managed threads they spawned
            self._kill_managed_threads()

            self.started = False

            # if `kill` is called after `stop`, they race to send this
            if not self._died.ready():
                self._died.send(None)

    def kill(self, exc_info=None):
        """ Kill the container in a semi-graceful way.

        Entrypoints are killed, followed by any active worker threads.
        Next, dependencies are killed. Finally, any remaining managed threads
        are killed.

        If ``exc_info`` is provided, the exception will be raised by
        :meth:`~wait``.
        """
        if self._being_killed:
            # this happens if a managed thread exits with an exception
            # while the container is being killed or if multiple errors
            # happen simultaneously
            _log.debug('already killing %s ... waiting for death', self)
            try:
                self._died.wait()
            except:
                pass  # don't re-raise if we died with an exception
            return

        self._being_killed = True

        if self._died.ready():
            _log.debug('already stopped %s', self)
            return

        if exc_info is not None:
            _log.info('killing %s due to %s', self, exc_info[1])
        else:
            _log.info('killing %s', self)

        # protect against extensions that throw during kill; the container
        # is already dying with an exception, so ignore anything else
        def safely_kill_extensions(ext_set):
            try:
                ext_set.kill()
            except Exception as exc:
                _log.warning('Extension raised `%s` during kill', exc)

        safely_kill_extensions(self.entrypoints.all)
        self._kill_worker_threads()
        safely_kill_extensions(self.extensions.all)
        self._kill_managed_threads()

        self.started = False

        # if `kill` is called after `stop`, they race to send this
        if not self._died.ready():
            self._died.send(None, exc_info)

    def wait(self):
        """ Block until the container has been stopped.

        If the container was stopped due to an exception, ``wait()`` will
        raise it.

        Any unhandled exception raised in a managed thread or in the
        worker lifecycle (e.g. inside :meth:`DependencyProvider.worker_setup`)
        results in the container being ``kill()``ed, and the exception
        raised from ``wait()``.
        """
        return self._died.wait()

    def spawn_worker(self, entrypoint, args, kwargs,
                     context_data=None, handle_result=None):
        """ Spawn a worker thread for running the service method decorated
        by `entrypoint`.

        ``args`` and ``kwargs`` are used as parameters for the service method.

        ``context_data`` is used to initialize a ``WorkerContext``.

        ``handle_result`` is an optional function which may be passed
        in by the entrypoint. It is called with the result returned
        or error raised by the service method. If provided it must return a
        value for ``result`` and ``exc_info`` to propagate to dependencies;
        these may be different to those returned by the service method.
        """

        if self._being_killed:
            _log.info("Worker spawn prevented due to being killed")
            raise ContainerBeingKilled()

        service = self.service_cls()
        worker_ctx = WorkerContext(
            self, service, entrypoint, args, kwargs, data=context_data
        )

        _log.debug('spawning %s', worker_ctx)
        gt = self._worker_pool.spawn(
            self._run_worker, worker_ctx, handle_result
        )
        gt.link(self._handle_worker_thread_exited, worker_ctx)

        self._worker_threads[worker_ctx] = gt
        return worker_ctx

    def spawn_managed_thread(self, fn, identifier=None):
        """ Spawn a managed thread to run ``fn`` on behalf of an extension.
        The passed `identifier` will be included in logs related to this
        thread, and otherwise defaults to `fn.__name__`, if it is set.

        Any uncaught errors inside ``fn`` cause the container to be killed.

        It is the caller's responsibility to terminate their spawned threads.
        Threads are killed automatically if they are still running after
        all extensions are stopped during :meth:`ServiceContainer.stop`.

        Extensions should delegate all thread spawning to the container.
        """
        if identifier is None:
            identifier = getattr(fn, '__name__', "<unknown>")

        gt = eventlet.spawn(fn)
        self._managed_threads[gt] = identifier
        gt.link(self._handle_managed_thread_exited, identifier)
        return gt

    def _run_worker(self, worker_ctx, handle_result):
        _log.debug('setting up %s', worker_ctx)

        _log.debug('call stack for %s: %s',
                   worker_ctx, '->'.join(worker_ctx.call_id_stack))

        with _log_time('ran worker %s', worker_ctx):

            self._inject_dependencies(worker_ctx)
            self._worker_setup(worker_ctx)

            result = exc_info = None
            method_name = worker_ctx.entrypoint.method_name
            method = getattr(worker_ctx.service, method_name)
            try:

                _log.debug('calling handler for %s', worker_ctx)

                with _log_time('ran handler for %s', worker_ctx):
                    result = method(*worker_ctx.args, **worker_ctx.kwargs)
            except Exception as exc:
                if isinstance(exc, worker_ctx.entrypoint.expected_exceptions):
                    _log.warning(
                        '(expected) error handling worker %s: %s',
                        worker_ctx, exc, exc_info=True)
                else:
                    _log.exception(
                        'error handling worker %s: %s', worker_ctx, exc)
                exc_info = sys.exc_info()

            if handle_result is not None:
                _log.debug('handling result for %s', worker_ctx)

                with _log_time('handled result for %s', worker_ctx):
                    result, exc_info = handle_result(
                        worker_ctx, result, exc_info)

            with _log_time('tore down worker %s', worker_ctx):

                self._worker_result(worker_ctx, result, exc_info)

                # we don't need this any more, and breaking the cycle means
                # this can be reclaimed immediately, rather than waiting for a
                # gc sweep
                del exc_info

                self._worker_teardown(worker_ctx)

    def _inject_dependencies(self, worker_ctx):
        for provider in self.dependencies:
            dependency = provider.get_dependency(worker_ctx)
            setattr(worker_ctx.service, provider.attr_name, dependency)

    def _worker_setup(self, worker_ctx):
        for provider in self.dependencies:
            provider.worker_setup(worker_ctx)

    def _worker_result(self, worker_ctx, result, exc_info):
        _log.debug('signalling result for %s', worker_ctx)
        for provider in self.dependencies:
            provider.worker_result(worker_ctx, result, exc_info)

    def _worker_teardown(self, worker_ctx):
        for provider in self.dependencies:
            provider.worker_teardown(worker_ctx)

    def _kill_worker_threads(self):
        """ Kill any currently executing worker threads.

        See :meth:`ServiceContainer.spawn_worker`
        """
        num_workers = len(self._worker_threads)

        if num_workers:
            _log.warning('killing %s active workers(s)', num_workers)
            for worker_ctx, gt in list(self._worker_threads.items()):
                _log.warning('killing active worker for %s', worker_ctx)
                gt.kill()

    def _kill_managed_threads(self):
        """ Kill any currently executing managed threads.

        See :meth:`ServiceContainer.spawn_managed_thread`
        """
        num_threads = len(self._managed_threads)

        if num_threads:
            _log.warning('killing %s managed thread(s)', num_threads)
            for gt, identifier in list(self._managed_threads.items()):
                _log.warning('killing managed thread `%s`', identifier)
                gt.kill()

    def _handle_worker_thread_exited(self, gt, worker_ctx):
        self._worker_threads.pop(worker_ctx, None)
        self._handle_thread_exited(gt)

    def _handle_managed_thread_exited(self, gt, extension):
        self._managed_threads.pop(gt, None)
        self._handle_thread_exited(gt)

    def _handle_thread_exited(self, gt):
        try:
            gt.wait()

        except GreenletExit:
            # we don't care much about threads killed by the container
            # this can happen in stop() and kill() if extensions
            # don't properly take care of their threads
            _log.debug('%s thread killed by container', self)

        except Exception:
            _log.critical('%s thread exited with error', self, exc_info=True)
            # any uncaught error in a thread is unexpected behavior
            # and probably a bug in the extension or container.
            # to be safe we call self.kill() to kill our dependencies and
            # provide the exception info to be raised in self.wait().
            self.kill(sys.exc_info())

    def __repr__(self):
        service_name = self.service_name
        return '<ServiceContainer [{}] at 0x{:x}>'.format(
            service_name, id(self))
