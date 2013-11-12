from nameko.dependencies import InjectionProvider


class ParallelExecutor(object):
    def __init__(self, to_wrap=None):
        self.to_wrap = to_wrap

    @classmethod
    def submit(cls, func, *args, **kwargs):
        return func(*args, **kwargs)

    def __getattr__(self, item):
        # return something that, if used, will go via submit
        wrapped_attribute = getattr(self.to_wrap, item)
        if callable(wrapped_attribute):
            def wrapped_function(*args, **kwargs):
                ParallelExecutor.submit(wrapped_attribute, *args, **kwargs)
            return wrapped_function
        return wrapped_attribute


class ParalleliseProvider(InjectionProvider):
    def acquire_injection(self, worker_ctx):
        return ParallelExecutor
