from __future__ import absolute_import
from contextlib import contextmanager
from functools import partial, wraps

from logging import getLogger
from threading import Lock

from concurrent import futures
import greenlet
from nameko.dependencies import InjectionProvider, injection, DependencyFactory


_log = getLogger(__name__)


def try_wraps(func):
    do_wrap = wraps(func)

    def try_to_wrap(inner):
        try:
            return do_wrap(inner)
        except AttributeError:
            # Mock objects don't have all the attributes needed in order to
            # be wrapped.
            return inner

    return try_to_wrap


class ParallelProxyManager(object):
    def __init__(self, thread_provider):
        self.thread_provider = thread_provider

    @contextmanager
    def __call__(self, to_wrap):
        executor = ParallelExecutor(self.thread_provider)
        proxy = ParallelProxy(executor, to_wrap)
        with executor:
            yield proxy


class ParallelExecutor(futures.Executor):
    def __init__(self, thread_provider):
        self.thread_provider = thread_provider
        self._spawned_threads = set()
        self._shutdown = False
        self._shutdown_lock = Lock()

    def submit(self, func, *args, **kwargs):
        with self._shutdown_lock:
            if self._shutdown:
                raise RuntimeError('cannot schedule new futures after '
                                   'shutdown')

            f = futures.Future()

            @try_wraps(func)
            def do_function_call():
                # A failure on the function call doesn't propagate up and kill
                # the container: it's marked on the future only.
                try:
                    result = func(*args, **kwargs)
                except Exception as exc:
                    self._handle_call_complete(f, exc=exc)
                    return None, exc
                else:
                    self._handle_call_complete(f, result=result)
                    return result, None

            t = self.thread_provider.spawn_managed_thread(do_function_call)
            self._spawned_threads.add(t)
            t.link(partial(self._handle_thread_exited, f))

            # Thread with this future starts immediately: you have no
            # opportunity to cancel it
            f.set_running_or_notify_cancel()

            return f

    def _handle_call_complete(self, future, result=None, exc=None):
        if exc:
            future.set_exception(exc)
        else:
            future.set_result(result)

    def _handle_thread_exited(self, future, gt):
        with self._shutdown_lock:
            self._spawned_threads.remove(gt)
            try:
                gt.wait()
            except greenlet.GreenletExit as green_exit:
                # 'Normal' errors with the submitted function call are handled
                # by `_handle_call_complete`
                future.set_exception(green_exit)

    def shutdown(self, wait=True):
        """
        Call to ensure all spawned threads have finished.

        This method is called when automatically ParallelExecutor is used as a
        Context Manager
        """
        with self._shutdown_lock:
            self._shutdown = True
            if wait:
                for thread in self._spawned_threads:
                    thread.wait()


class ParallelProxy(object):
    def __init__(self, executor, to_wrap=None):
        """
        Create a new proxy around an object then ensures method calls are ran
        by the executor.

        Attribute access and function calls on the wrapped object can be
        performed as normal, but writing to fields
        is not allowed.

        You can use this as a context manager: it uses the associated executor
        as one.
        """
        self.executor = executor
        self.to_wrap = to_wrap

    def __getattr__(self, item):
        """
        Callables accessed on the wrapped object are performed by the executor
        calling `submit`
        """
        wrapped_attribute = getattr(self.to_wrap, item)
        if callable(wrapped_attribute):
            # Give name/docs of wrapped_attribute to do_submit
            @try_wraps(wrapped_attribute)
            def do_submit(*args, **kwargs):
                return self.executor.submit(wrapped_attribute, *args, **kwargs)
            return do_submit
        return wrapped_attribute


class ParallelProvider(InjectionProvider):
    def __init__(self):
        self.proxy_manager = None

    def acquire_injection(self, worker_ctx):
        self.proxy_manager = ParallelProxyManager(self.container)
        return self.proxy_manager


@injection
def parallel_provider(*args, **kwargs):
    return DependencyFactory(ParallelProvider, *args, **kwargs)
