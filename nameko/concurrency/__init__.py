"""Concurrency abstractions.

Allows concurrency primitives to be abstracted such that both eventlet and
gevent can be used to run nameko. The backend is chosen at import time and may
be set using the global nameko config object. Valid values are "eventlet" and
"gevent".

To write nameko code and extensions that is compatible with multiple
concurrency backend, use this compatability layer package instead of importing
from a specific backend:

NO:

>>> from eventlet import sleep

YES:

>>> from nameko.concurrency import sleep

"""
import os
import sys

from nameko import config
from nameko.constants import (
    CONCURRENCY_BACKEND_CONFIG_KEY, DEFAULT_CONCURRENCY_BACKEND,
    MONKEY_PATCH_ENABLED_CONFIG_KEY, DEFAULT_MONKEY_PATCH_ENABLED
)

mode = os.environ.get(
    CONCURRENCY_BACKEND_CONFIG_KEY,
    config.get(CONCURRENCY_BACKEND_CONFIG_KEY, DEFAULT_CONCURRENCY_BACKEND)
)

if mode == 'eventlet':  # pragma: no cover
    try:
        import eventlet  # noqa: F401
    except ImportError as e:
        raise type(e)('Could not import eventlet. '
                      'Try installing concurrency backend by running '
                      'pip install nameko[eventlet].')

    from nameko.concurrency.eventlet_backend import (
        getcurrent,
        spawn,
        spawn_n,
        spawn_after,
        sleep,
        Event,
        Pool,
        Queue,
        monkey_patch,
        Semaphore,
        Timeout,
        resize_queue,
        wait,
        get_waiter_count,
        listen,
        setup_backdoor,
        get_wsgi_server,
        process_wsgi_request,
        HttpOnlyProtocol,
        yield_thread,
    )

elif mode == 'gevent':  # pragma: no cover
    try:
        import eventlet  # noqa: F401
    except ImportError as e:
        raise type(e)('Could not import eventlet. '
                      'Try installing concurrency backend by running '
                      '`pip install nameko[gevent]`.')

    from nameko.concurrency.gevent_backend import (
        getcurrent,
        spawn,
        spawn_n,
        spawn_after,
        sleep,
        Event,
        Pool,
        Queue,
        monkey_patch,
        Semaphore,
        Timeout,
        resize_queue,
        wait,
        get_waiter_count,
        listen,
        setup_backdoor,
        get_wsgi_server,
        process_wsgi_request,
        HttpOnlyProtocol,
        yield_thread,
    )
else:  # pragma: no cover
    raise NotImplementedError(
        "Concurrency backend '{}' is not available. Choose 'eventlet' or 'gevent'."
        .format(mode)
    )


def monkey_patch_if_enabled():
    monkey_patch_enabled = config.get(
        MONKEY_PATCH_ENABLED_CONFIG_KEY,
        DEFAULT_MONKEY_PATCH_ENABLED
    )
    if monkey_patch_enabled:
        monkey_patch()


def fail_fast_imap(pool, call, items):
    """ Run a function against each item in a given list, yielding each
    function result in turn, where the function call is handled in a
    :class:`~eventlet.greenthread.GreenThread` spawned by the provided pool.

    If any function raises an exception, all other ongoing threads are killed,
    and the exception is raised to the caller.

    This function is similar to :meth:`~eventlet.greenpool.GreenPool.imap`.

    :param pool: Pool to spawn function threads from
    :type pool: nameko.concurrency.Pool
    :param call: Function call to make, expecting to receive an item from the
        given list
    """
    result_queue = Queue(maxsize=len(items))
    spawned_threads = set()

    def handle_result(finished_thread):
        try:
            thread_result = wait(finished_thread)
            spawned_threads.remove(finished_thread)
            result_queue.put((thread_result, None))
        except Exception:
            spawned_threads.remove(finished_thread)
            result_queue.put((None, sys.exc_info()))

    for item in items:
        gt = pool.spawn(call, item)
        spawned_threads.add(gt)
        gt.link(handle_result)

    while spawned_threads:
        result, exc_info = result_queue.get()
        if exc_info is not None:
            # Kill all other ongoing threads
            for ongoing_thread in spawned_threads:
                ongoing_thread.kill()
            # simply raising here (even raising a full exc_info) isn't
            # sufficient to preserve the original stack trace.
            # greenlet.throw() achieves this.
            getcurrent().throw(*exc_info)
        yield result


class SpawningProxy(object):
    def __init__(self, items, abort_on_error=False):
        """ Wraps an iterable set of items such that a call on the returned
        SpawningProxy instance will spawn a call in a
        :class:`~eventlet.greenthread.GreenThread` for each item.

        Returns when every spawned thread has completed.

        :param items: Iterable item set to process
        :param abort_on_error: If True, any exceptions raised on an individual
            item call will cause all peer item call threads to be killed, and
            for the exception to be propagated to the caller immediately.
        """
        self._items = items
        self.abort_on_error = abort_on_error

    def __getattr__(self, name):

        def spawning_method(*args, **kwargs):
            items = self._items
            if items:
                pool = Pool(len(items))

                def call(item):
                    return getattr(item, name)(*args, **kwargs)

                if self.abort_on_error:
                    return list(fail_fast_imap(pool, call, self._items))
                else:
                    return list(pool.imap(call, self._items))
        return spawning_method


class SpawningSet(set):
    """ A set with an ``.all`` property that will spawn a method call on each
    item in the set into its own (parallel) greenthread.
    """
    @property
    def all(self):
        return SpawningProxy(self)


__all__ = [
    'getcurrent',
    'spawn',
    'spawn_n',
    'spawn_after',
    'sleep',
    'Event',
    'Pool',
    'Queue',
    'monkey_patch',
    'Semaphore',
    'Timeout',
    'resize_queue',
    'wait',
    'get_waiter_count',
    'listen',
    'setup_backdoor',
    'get_wsgi_server',
    'process_wsgi_request',
    'HttpOnlyProtocol',
    'yield_thread',
    'monkey_patch_if_enabled',
    'fail_fast_imap',
    'SpawningProxy',
    'SpawningSet',
]
