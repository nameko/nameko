from __future__ import absolute_import, unicode_literals

from abc import ABCMeta, abstractproperty
import inspect
from logging import getLogger
import sys
import uuid

import eventlet
from eventlet.event import Event
from eventlet.greenpool import GreenPool
from greenlet import GreenletExit  # pylint: disable=E0611
import six

from nameko.constants import (
    PARENT_CALLS_CONFIG_KEY, DEFAULT_PARENT_CALLS_TRACKED,
    MAX_WORKERS_CONFIG_KEY, DEFAULT_MAX_WORKERS,
    SERIALIZER_CONFIG_KEY, DEFAULT_SERIALIZER,
    CALL_ID_STACK_CONTEXT_KEY, NAMEKO_CONTEXT_KEYS)

from nameko.extensions import (
    is_dependency, ENTRYPOINT_EXTENSIONS_ATTR, iter_extensions)
from nameko.exceptions import ContainerBeingKilled, ConfigurationError
from nameko.log_helpers import make_timing_logger
from nameko.utils import SpawningSet

_log = getLogger(__name__)
_log_time = make_timing_logger(_log)
MANAGED_THREAD = object()

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


def new_call_id():
    return str(uuid.uuid4())


class WorkerContextBase(object):
    """ Abstract base class for a WorkerContext
    """
    __metaclass__ = ABCMeta

    def __init__(self, container, service, entrypoint, args=None, kwargs=None,
                 data=None):
        self.container = container
        self.config = container.config  # TODO: remove?

        self.parent_calls_tracked = self.config.get(
            PARENT_CALLS_CONFIG_KEY, DEFAULT_PARENT_CALLS_TRACKED)

        self.service = service
        self.entrypoint = entrypoint
        self.service_name = self.container.service_name

        self.args = args if args is not None else ()
        self.kwargs = kwargs if kwargs is not None else {}

        self.data = data if data is not None else {}

        self.parent_call_stack, self.unique_id = self._init_call_id()
        self.call_id = '{}.{}.{}'.format(
            self.service_name, self.entrypoint.method_name, self.unique_id
        )
        n = -self.parent_calls_tracked
        self.call_id_stack = self.parent_call_stack[n:]
        self.call_id_stack.append(self.call_id)
        try:
            self.immediate_parent_call_id = self.parent_call_stack[-1]
        except IndexError:
            self.immediate_parent_call_id = None

    @abstractproperty
    def context_keys(self):
        """ Return a tuple of keys describing data kept on this WorkerContext.
        """

    @property
    def context_data(self):
        """
        Contextual data to pass with each call originating from the active
        worker.

        Comprises items from ``self.data`` where the key is included in
        ``context_keys``, as well as the call stack.
        """
        key_data = {k: v for k, v in six.iteritems(self.data)
                    if k in self.context_keys}
        key_data[CALL_ID_STACK_CONTEXT_KEY] = self.call_id_stack
        return key_data

    @classmethod
    def get_context_data(cls, incoming):
        data = {k: v for k, v in six.iteritems(incoming)
                if k in cls.context_keys}
        return data

    def __repr__(self):
        cls_name = type(self).__name__
        service_name = self.service_name
        method_name = self.entrypoint.method_name
        return '<{} [{}.{}] at 0x{:x}>'.format(
            cls_name, service_name, method_name, id(self))

    def _init_call_id(self):
        parent_call_stack = self.data.pop(CALL_ID_STACK_CONTEXT_KEY, [])
        unique_id = new_call_id()
        return parent_call_stack, unique_id


class WorkerContext(WorkerContextBase):
    """ Default WorkerContext implementation
    """
    context_keys = NAMEKO_CONTEXT_KEYS


class ServiceContainer(object):

    def __init__(self, service_cls, config, worker_ctx_cls=None):

        self.service_cls = service_cls
        self.config = config

        if worker_ctx_cls is None:
            worker_ctx_cls = WorkerContext
        self.worker_ctx_cls = worker_ctx_cls

        self.service_name = get_service_name(service_cls)
        self.shared_extensions = {}

        self.max_workers = (
            config.get(MAX_WORKERS_CONFIG_KEY) or DEFAULT_MAX_WORKERS)

        self.serializer = config.get(
            SERIALIZER_CONFIG_KEY, DEFAULT_SERIALIZER)

        self.accept = [self.serializer]

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

        self._active_threads = {}
        self._protected_threads = set()
        self._being_killed = False
        self._died = Event()

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

            # just in case there was an entrypoint not taking care of its
            # workers, or an extension not taking care of its managed threads
            self._kill_active_threads()
            self._kill_protected_threads()

            self.started = False

            # if `kill` is called after `stop`, they race to send this
            if not self._died.ready():
                self._died.send(None)

    def kill(self, exc_info=None):
        """ Kill the container in a semi-graceful way.

        All non-protected managed threads are killed first. This includes
        all active workers generated by :meth:`ServiceContainer.spawn_worker`.
        Next, dependencies are killed. Finally, any remaining protected threads
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
        self._kill_active_threads()
        safely_kill_extensions(self.extensions.all)
        self._kill_protected_threads()

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
        with by `entrypoint`.

        ``args`` and ``kwargs`` are used as arguments for the service
        method.

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
        worker_ctx = self.worker_ctx_cls(
            self, service, entrypoint, args, kwargs, data=context_data)

        _log.debug('spawning %s', worker_ctx)
        gt = self._worker_pool.spawn(self._run_worker, worker_ctx,
                                     handle_result)
        self._active_threads[gt] = entrypoint
        gt.link(self._handle_thread_exited)
        return worker_ctx

    def spawn_managed_thread(self, run_method, protected=False):
        """ Spawn a managed thread to run ``run_method``.

        Threads can be marked as ``protected``, which means the container will
        not forcibly kill them until after all extensions have been killed.
        Extensions that require a managed thread to complete their kill
        procedure should ensure to mark them as ``protected``. For example,
        :class:`nameko.messaging.QueueConsumer` cleanly closes connections
        to the AMQP broker even when killed; the thread that holds that
        connection is ``protected`` so it isn't stopped until after the
        QueueConsumer returns from its kill.

        Any uncaught errors inside ``run_method`` cause the container to be
        killed.

        It is the caller's responsibility to terminate their spawned threads.
        Threads are killed automatically if they are still running after
        all extensions are stopped during :meth:`ServiceContainer.stop`.

        Extensions should delegate all thread spawning to the container.
        """
        gt = eventlet.spawn(run_method)
        if not protected:
            self._active_threads[gt] = MANAGED_THREAD
        else:
            self._protected_threads.add(gt)
        gt.link(self._handle_thread_exited)
        return gt

    def _run_worker(self, worker_ctx, handle_result):
        _log.debug('setting up %s', worker_ctx)

        if not worker_ctx.parent_call_stack:
            _log.debug('starting call chain')
        _log.debug('call stack for %s: %s',
                   worker_ctx, '->'.join(worker_ctx.call_id_stack))

        with _log_time('ran worker %s', worker_ctx):

            # when we have better parallelization than ``spawningset``,
            # do this injection inline
            self.dependencies.all.inject(worker_ctx)
            self.dependencies.all.worker_setup(worker_ctx)

            result = exc_info = None
            method_name = worker_ctx.entrypoint.method_name
            method = getattr(worker_ctx.service, method_name)
            try:

                _log.debug('calling handler for %s', worker_ctx)

                with _log_time('ran handler for %s', worker_ctx):
                    result = method(*worker_ctx.args, **worker_ctx.kwargs)
            except Exception as exc:
                _log.info('error handling worker %s: %s', worker_ctx, exc,
                          exc_info=True)
                exc_info = sys.exc_info()

            if handle_result is not None:
                _log.debug('handling result for %s', worker_ctx)

                with _log_time('handled result for %s', worker_ctx):
                    result, exc_info = handle_result(
                        worker_ctx, result, exc_info)

            with _log_time('tore down worker %s', worker_ctx):

                _log.debug('signalling result for %s', worker_ctx)
                self.dependencies.all.worker_result(
                    worker_ctx, result, exc_info)

                # we don't need this any more, and breaking the cycle means
                # this can be reclaimed immediately, rather than waiting for a
                # gc sweep
                del exc_info

                self.dependencies.all.worker_teardown(worker_ctx)
                # release?

    def _kill_active_threads(self):
        """ Kill all managed threads that were not marked as "protected" when
        they were spawned.

        This set will include all worker threads generated by
        :meth:`ServiceContainer.spawn_worker`.

        See :meth:`ServiceContainer.spawn_managed_thread`
        """
        num_active_threads = len(self._active_threads)

        if num_active_threads:
            _log.warning('killing %s active thread(s)', num_active_threads)
            for gt, extension in list(self._active_threads.items()):
                if extension is not MANAGED_THREAD:
                    _log.warning('killing active thread for %s', extension)
                gt.kill()

    def _kill_protected_threads(self):
        """ Kill any managed threads marked as protected when they were
        spawned.

        See :meth:`ServiceContainer.spawn_managed_thread`
        """
        num_protected_threads = len(self._protected_threads)

        if num_protected_threads:
            _log.warning('killing %s protected thread(s)',
                         num_protected_threads)
            for gt in list(self._protected_threads):
                gt.kill()

    def _handle_thread_exited(self, gt):
        self._active_threads.pop(gt, None)
        self._protected_threads.discard(gt)

        try:
            gt.wait()

        except GreenletExit:
            # we don't care much about threads killed by the container
            # this can happen in stop() and kill() if extensions
            # don't properly take care of their threads
            _log.debug('%s thread killed by container', self)

        except Exception:
            _log.error('%s thread exited with error', self, exc_info=True)
            # any error raised inside an active thread is unexpected behavior
            # and probably a bug in the extension or container.
            # to be safe we call self.kill() to kill our dependencies and
            # provide the exception info to be raised in self.wait().
            self.kill(sys.exc_info())

    def __repr__(self):
        service_name = self.service_name
        return '<ServiceContainer [{}] at 0x{:x}>'.format(
            service_name, id(self))
