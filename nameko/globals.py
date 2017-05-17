from contextlib import contextmanager
from werkzeug.local import LocalStack


_workers_stack = LocalStack()
worker_ctx = _workers_stack()


@contextmanager
def push_worker_ctx(worker_ctx_):
    _workers_stack.push(worker_ctx_)
    try:
        yield worker_ctx_
    finally:
        _workers_stack.pop()
