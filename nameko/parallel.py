from concurrent.futures import _base
from nameko.dependencies import InjectionProvider


class BadFuture(object):
    def __init__(self, thread):
        self.thread = thread

    def result(self):
        return self.thread.wait()


class ParallelExecutor(_base.Executor):
    def __init__(self, thread_provider):
        self.thread_provider = thread_provider
        self.spawned_threads = set()

    def submit(self, func, *args, **kwargs):
        def do_function_call():
            return func(*args, **kwargs)

        # TODO: Returning a poor version of Future
        t = self.thread_provider.spawn_managed_thread(do_function_call)
        self.spawned_threads.add(t)

        future = BadFuture(t)
        return future

    def __call__(self, to_wrap):
        """
        Provides a wrapper around the provided object that ensures any method calls on it are handled by the `submit`
        method of this executor.
        """
        return ParallelWrapper(self, to_wrap)

    def shutdown(self, wait=True):
        """
        Call to ensure all spawned threads have finished.

        This method is called when automatically ParallelExecutor is used as a Context Manager
        """
        if wait:
            for thread in self.spawned_threads:
                thread.wait()


class ParallelWrapper(object):
    def __init__(self, executor, to_wrap=None):
        """
        Create a new wrapper around an object then ensures method calls are ran by the executor.

        Attribute access and function calls on the wrapped object can be performed as normal, but writing to fields is not allowed.

        You can use this as a context manager: it uses the associated executor as one.
        """
        self.executor = executor
        self.to_wrap = to_wrap

    def __getattr__(self, item):
        """
        Callables accessed on the wrapped object are performed by the executor calling `submit`
        """
        wrapped_attribute = getattr(self.to_wrap, item)
        if callable(wrapped_attribute):
            def do_submit(*args, **kwargs):
                return self.executor.submit(wrapped_attribute, *args, **kwargs)
            return do_submit
        return wrapped_attribute

    def __enter__(self):
        self.executor.__enter__()
        return self

    def __exit__(self, *args, **kwargs):
        self.executor.__exit__(*args, **kwargs)
        return


class ParalleliseProvider(InjectionProvider):
    def acquire_injection(self, worker_ctx):
        return ParallelWrapper
