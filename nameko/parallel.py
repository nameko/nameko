from __future__ import absolute_import
from contextlib import contextmanager
from functools import partial

from logging import getLogger

from concurrent import futures
import greenlet
from nameko.utils import try_wraps
from nameko.dependencies import InjectionProvider, injection, DependencyFactory


_log = getLogger(__name__)


class ParallelProxyFactory(object):
    def __init__(self, thread_provider):
        self.container = thread_provider

    @contextmanager
    def __call__(self, to_wrap):
        executor = ParallelExecutor(self.container)
        proxy = ParallelProxy(executor, to_wrap)
        with executor:
            yield proxy


class ParallelExecutor(futures.Executor):
    def __init__(self, container):
        self.container = container
        self._spawned_threads = set()
        self._shutdown = False

    def submit(self, func, *args, **kwargs):
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

        gt = self.container.spawn_managed_thread(do_function_call)
        self._spawned_threads.add(gt)
        gt.link(partial(self._handle_thread_exited, f))

        # Mark the future as running immediately. It's not possible for it
        # to have been cancelled.
        f.set_running_or_notify_cancel()

        return f

    def _handle_call_complete(self, future, result=None, exc=None):
        """
        Set a result or exception on ``future``

        Called when the future's function completes, inside the spawned thread.
        """
        if exc:
            future.set_exception(exc)
        else:
            future.set_result(result)

    def _handle_thread_exited(self, future, gt):
        """
        Handle the completion of a thread spawned for a future.
        """
        self._spawned_threads.remove(gt)
        try:
            gt.wait()
        except greenlet.GreenletExit as green_exit:
            # 'Normal' errors with the submitted function call are handled
            # by `_handle_call_complete`. This only occurs when the
            # container is killed before the future's thread exits.
            future.set_exception(green_exit)

    def shutdown(self, wait=True):
        """
        Call to ensure all spawned threads have finished.

        This method is called when automatically ParallelExecutor is used as a
        Context Manager
        """
        self._shutdown = True
        if wait:
            # Fix the list of threads to wait for, so threads that remove while
            # waiting don't change the iterator
            for thread in list(self._spawned_threads):
                thread.wait()


class ParallelProxy(object):
    def __init__(self, executor, to_wrap=None):
        """
        Create a new proxy around an object then ensures method calls are ran
        by the executor.

        Attribute access and function calls on the wrapped object can be
        performed as normal, but writing to fields is not allowed.

        You can use this as a context manager: it uses the associated executor
        as one.
        """
        self.__dict__['executor'] = executor
        self.__dict__['to_wrap'] = to_wrap

    def __getattr__(self, name):
        """
        Callables accessed on the wrapped object are performed by the executor
        calling `submit`
        """
        wrapped_attribute = getattr(self.to_wrap, name)
        if callable(wrapped_attribute):
            # Give name/docs of wrapped_attribute to do_submit
            @try_wraps(wrapped_attribute)
            def do_submit(*args, **kwargs):
                return self.executor.submit(wrapped_attribute, *args, **kwargs)
            return do_submit
        return wrapped_attribute

    def __setattr__(self, key, value):
        """
        Attribute setting is unsupported on proxy objects
        """
        raise ProxySettingUnsupportedException()


class ProxySettingUnsupportedException(Exception):
    pass


class ParallelProvider(InjectionProvider):
    def acquire_injection(self, worker_ctx):
        return ParallelProxyFactory(self.container)


@injection
def parallel_provider(*args, **kwargs):
    return DependencyFactory(ParallelProvider, *args, **kwargs)
