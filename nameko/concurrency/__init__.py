"""Concurrency."""

mode = 'eventlet'

if mode == 'eventlet':
    from nameko.concurrency.eventlet import (
        getcurrent,
        spawn,
        spawn_n,
        spawn_after,
        sleep,
        Event,
        GreenPool,
        Queue,
        monkey_patch,
        Semaphore,
        Timeout,
        wait,
        listen,
        setup_backdoor
    )

elif mode == 'gevent':
    # Differences to eventlet:
    # Link executes AFTER parent thread has returned (in eventlet it executes
    # in the same context)
    from nameko.concurrency.gevent import (
        getcurrent,
        spawn,
        spawn_n,
        spawn_after,
        sleep,
        Event,
        GreenPool,
        Queue,
        monkey_patch,
        Semaphore,
        Timeout,
        wait,
        listen,
        setup_backdoor
    )


__all__ = [
    'getcurrent',
    'spawn',
    'spawn_n',
    'spawn_after',
    'sleep',
    'Event',
    'GreenPool',
    'Queue',
    'monkey_patch',
    'Semaphore',
    'Timeout',
    'wait',
    'listen',
    'setup_backdoor'
]
