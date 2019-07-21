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

from nameko import config
from nameko.constants import CONCURRENCY_BACKEND_CONFIG_KEY, DEFAULT_CONCURRENCY_BACKEND

mode = os.environ.get(
    CONCURRENCY_BACKEND_CONFIG_KEY,
    config.get(CONCURRENCY_BACKEND_CONFIG_KEY, DEFAULT_CONCURRENCY_BACKEND)
)

if mode == 'eventlet':
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
    )

elif mode == 'gevent':
    # Differences to eventlet:
    # Link executes AFTER parent thread has returned (in eventlet it executes
    # in the same context)
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
    )
else:
    raise NotImplementedError(
        "Concurrency backend '{}' is not available. Choose 'eventlet' or 'gevent'."
        .format(mode)
    )


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
]
