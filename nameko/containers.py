from __future__ import absolute_import

from abc import ABCMeta, abstractproperty
from logging import getLogger
import sys
import uuid

import eventlet
from eventlet.event import Event
from eventlet.greenpool import GreenPool
import greenlet

from nameko.constants import (
    PARENT_CALLS_CONFIG_KEY, DEFAULT_PARENT_CALLS_TRACKED,
    MAX_WORKERS_CONFIG_KEY, DEFAULT_MAX_WORKERS,
    CALL_ID_STACK_CONTEXT_KEY, NAMEKO_CONTEXT_KEYS)

from nameko.dependencies import (
    prepare_dependencies, DependencySet, is_entrypoint_provider,
    is_injection_provider)
from nameko.exceptions import ContainerBeingKilled
from nameko.logging import log_time


_log = getLogger(__name__)


def get_service_name(service_cls):
    return getattr(service_cls, "name", service_cls.__name__.lower())


def new_call_id():
    return str(uuid.uuid4())


class WorkerContextBase(object):
    """ Abstract base class for a WorkerContext
    """
    __metaclass__ = ABCMeta

    def __init__(self, container, service, provider, args=None, kwargs=None,
                 data=None):
        self.container = container
        self.config = container.config  # TODO: remove?

        self.parent_calls_tracked = self.config.get(
            PARENT_CALLS_CONFIG_KEY, DEFAULT_PARENT_CALLS_TRACKED)

        self.service = service
        self.provider = provider
        self.service_name = self.container.service_name

        self.args = args if args is not None else ()
        self.kwargs = kwargs if kwargs is not None else {}

        self.data = data if data is not None else {}

        self.parent_call_stack, self.unique_id = self._init_call_id()
        self.call_id = '{}.{}.{}'.format(
            self.service_name, self.provider.name, self.unique_id
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
        key_data = {k: v for k, v in self.data.iteritems()
                    if k in self.context_keys}
        key_data[CALL_ID_STACK_CONTEXT_KEY] = self.call_id_stack
        return key_data

    @property
    def extra_for_logging(self):
        """
        Get a dictionary of extra data to apply to log statements.
        """
        return {}

    @classmethod
    def get_context_data(cls, incoming):
        data = {k: v for k, v in incoming.iteritems()
                if k in cls.context_keys}
        return data

    def __str__(self):
        cls_name = type(self).__name__
        return '<{} {}.{} at 0x{:x}>'.format(
            cls_name, self.service_name, self.provider.name, id(self))

    def _init_call_id(self):
        parent_call_stack = self.data.pop(CALL_ID_STACK_CONTEXT_KEY, [])
        unique_id = new_call_id()
        return parent_call_stack, unique_id


class WorkerContext(WorkerContextBase):
    """ Default WorkerContext implementation
    """
    context_keys = NAMEKO_CONTEXT_KEYS


class ServiceContainer(object):

    def __init__(self, service_cls, worker_ctx_cls, config):

        self.service_cls = service_cls
        self.worker_ctx_cls = worker_ctx_cls

        self.service_name = get_service_name(service_cls)

        self.config = config
        self.max_workers = (
            config.get(MAX_WORKERS_CONFIG_KEY) or DEFAULT_MAX_WORKERS)

        self.dependencies = DependencySet()
        for dep in prepare_dependencies(self):
            self.dependencies.add(dep)

        self.started = False
        self._worker_pool = GreenPool(size=self.max_workers)

        self._active_threads = set()
        self._protected_threads = set()
        self._being_killed = False
        self._died = Event()

    @property
    def entrypoints(self):
        return filter(is_entrypoint_provider, self.dependencies)

    @property
    def injections(self):
        return filter(is_injection_provider, self.dependencies)

    def start(self):
        """ Start a container by starting all the dependency providers.
        """
        _log.debug('starting %s', self)
        self.started = True

        with log_time(_log.debug, 'started %s in %0.3f sec', self):
            self.dependencies.all.prepare()
            self.dependencies.all.start()

    def stop(self):
        """ Stop the container gracefully.

        First all entrypoints are asked to ``stop()``.
        This ensures that no new worker threads are started.

        It is the providers' responsibility to gracefully shut down when
        ``stop()`` is called on them and only return when they have stopped.

        After all entrypoints have stopped the container waits for any
        active workers to complete.

        After all active workers have stopped the container stops all
        injections.

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

        with log_time(_log.debug, 'stopped %s in %0.3f sec', self):
            dependencies = self.dependencies

            # entrypoint deps have to be stopped before injection deps
            # to ensure that running workers can successfully complete
            dependencies.entrypoints.all.stop()

            # there might still be some running workers, which we have to
            # wait for to complete before we can stop injection dependencies
            self._worker_pool.waitall()

            # it should be safe now to stop any injection as there is no
            # active worker which could be using it
            dependencies.injections.all.stop()

            # finally, stop nested dependencies
            dependencies.nested.all.stop()

            # just in case there was a provider not taking care of its workers,
            # or a dependency not taking care of its protected threads
            self._kill_active_threads()
            self._kill_protected_threads()

            self.started = False
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

        self.dependencies.entrypoints.all.kill()
        self._kill_active_threads()
        self.dependencies.all.kill()
        self._kill_protected_threads()

        self.started = False
        self._died.send(None, exc_info)

    def wait(self):
        """ Block until the container has been stopped.

        If the container was stopped due to an exception, ``wait()`` will
        raise it.

        Any unhandled exception raised in a managed thread or in the
        life-cycle management code also causes the container to be
        ``kill()``ed, which causes an exception to be raised from ``wait()``.
        """
        return self._died.wait()

    def spawn_worker(self, provider, args, kwargs,
                     context_data=None, handle_result=None):
        """ Spawn a worker thread for running the service method decorated
        with an entrypoint ``provider``.

        ``args`` and ``kwargs`` are used as arguments for the service
        method.

        ``context_data`` is used to initialize a ``WorkerContext``.

        ``handle_result`` is an optional callback which may be passed
        in by the calling entrypoint provider. It is called with the
        result returned or error raised by the service method.
        """

        if self._being_killed:
            _log.info("Worker spawn prevented due to being killed")
            raise ContainerBeingKilled()

        service = self.service_cls()
        worker_ctx = self.worker_ctx_cls(
            self, service, provider, args, kwargs, data=context_data)

        _log.debug('spawning %s', worker_ctx,
                   extra=worker_ctx.extra_for_logging)
        gt = self._worker_pool.spawn(self._run_worker, worker_ctx,
                                     handle_result)
        self._active_threads.add(gt)
        gt.link(self._handle_thread_exited)
        return worker_ctx

    def spawn_managed_thread(self, run_method, protected=False):
        """ Spawn a managed thread to run ``run_method``.

        Threads can be marked as ``protected``, which means the container will
        not forcibly kill them until after all dependencies have been killed.
        Dependencies that require a managed thread to complete their kill
        procedure should ensure to mark them as ``protected``.

        Any uncaught errors inside ``run_method`` cause the container to be
        killed.

        It is the caller's responsibility to terminate their spawned threads.
        Threads are killed automatically if they are still running after
        all dependencies are stopped during :meth:`ServiceContainer.stop`.

        Entrypoints may only create separate threads using this method,
        to ensure they are life-cycle managed.
        """
        gt = eventlet.spawn(run_method)
        if not protected:
            self._active_threads.add(gt)
        else:
            self._protected_threads.add(gt)
        gt.link(self._handle_thread_exited)
        return gt

    def _run_worker(self, worker_ctx, handle_result):
        _log.debug('setting up %s', worker_ctx,
                   extra=worker_ctx.extra_for_logging)

        if not worker_ctx.parent_call_stack:
            _log.debug('starting call chain',
                       extra=worker_ctx.extra_for_logging)
        _log.debug('call stack for %s: %s',
                   worker_ctx, '->'.join(worker_ctx.call_id_stack),
                   extra=worker_ctx.extra_for_logging)

        with log_time(_log.debug, 'ran worker %s in %0.3fsec', worker_ctx):

            self.dependencies.injections.all.inject(worker_ctx)
            self.dependencies.all.worker_setup(worker_ctx)

            result = exc_info = None
            method = getattr(worker_ctx.service, worker_ctx.provider.name)
            try:

                _log.debug('calling handler for %s', worker_ctx,
                           extra=worker_ctx.extra_for_logging)

                with log_time(_log.debug, 'ran handler for %s in %0.3fsec',
                              worker_ctx):
                    result = method(*worker_ctx.args, **worker_ctx.kwargs)
            except Exception as exc:
                _log.debug('error handling worker %s: %s', worker_ctx, exc,
                           exc_info=True, extra=worker_ctx.extra_for_logging)
                exc_info = sys.exc_info()

            if handle_result is not None:
                _log.debug('handling result for %s', worker_ctx,
                           extra=worker_ctx.extra_for_logging)

                with log_time(_log.debug, 'handled result for %s in %0.3fsec',
                              worker_ctx):
                    handle_result(worker_ctx, result, exc_info)

            with log_time(_log.debug, 'tore down worker %s in %0.3fsec',
                          worker_ctx):

                _log.debug('signalling result for %s', worker_ctx,
                           extra=worker_ctx.extra_for_logging)
                self.dependencies.injections.all.worker_result(
                    worker_ctx, result, exc_info)

                _log.debug('tearing down %s', worker_ctx,
                           extra=worker_ctx.extra_for_logging)
                self.dependencies.all.worker_teardown(worker_ctx)
                self.dependencies.injections.all.release(worker_ctx)

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
            for gt in list(self._active_threads):
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
        self._active_threads.discard(gt)
        self._protected_threads.discard(gt)

        try:
            gt.wait()

        except greenlet.GreenletExit:
            # we don't care much about threads killed by the container
            # this can happen in stop() and kill() if providers
            # don't properly take care of their threads
            _log.warning('%s thread killed by container', self)

        except Exception:
            _log.error('%s thread exited with error', self, exc_info=True)
            # any error raised inside an active thread is unexpected behavior
            # and probably a bug in the providers or container.
            # to be safe we call self.kill() to kill our dependencies and
            # provide the exception info to be raised in self.wait().
            self.kill(sys.exc_info())

    def __str__(self):
        return '<ServiceContainer [{}] at 0x{:x}>'.format(
            self.service_name, id(self))
