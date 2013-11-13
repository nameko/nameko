from concurrent.futures import _base
from nameko.dependencies import InjectionProvider


class ParallelExecutor(_base.Executor):
    def __init__(self, thread_provider):
        self.thread_provider = thread_provider
        self.spawned_threads = set()

    def submit(self, func, *args, **kwargs):
        def do_function_call():
            return func(*args, **kwargs)
        # TODO: Returning green thread, not future...
        t = self.thread_provider.spawn_managed_thread(do_function_call)
        self.spawned_threads.add(t)
        return t

    def __call__(self, to_wrap):
        return ParallelWrapper(self, to_wrap)

    def shutdown(self, wait=True):
        # Wait for all spawned threads to have finished
        if wait:
            for thread in self.spawned_threads:
                thread.wait()


class ParallelWrapper(object):
    def __init__(self, executor, to_wrap=None):
        self.executor = executor
        self.to_wrap = to_wrap

    def __getattr__(self, item):
        # return something that, if a callable, will go via submit
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
