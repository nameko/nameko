from nameko.dependencies import InjectionProvider


class ParallelExecutor(object):
    def submit(self, func, *args, **kwargs):
        return func(*args, **kwargs)

    def __call__(self, to_wrap):
        return ParallelWrapper(self, to_wrap)

    def __exit__(self, exc_type, exc_val, exc_tb):
        return

    def __enter__(self):
        return self


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
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return


class ParalleliseProvider(InjectionProvider):
    def acquire_injection(self, worker_ctx):
        return ParallelWrapper
