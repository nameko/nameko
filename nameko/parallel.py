from nameko.dependencies import InjectionProvider


class ParallelExecutor(object):
    @classmethod
    def submit(cls, func, *args, **kwargs):
        return func(*args, **kwargs)


class ParalleliseProvider(InjectionProvider):
    def acquire_injection(self, worker_ctx):
        return ParallelExecutor
