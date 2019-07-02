"""Concurrency."""

mode = 'eventlet'

if mode == 'eventlet':
    from nameko.concurrency.eventlet import *

elif mode == 'gevent':
    # Differences to eventlet:
    # Link executes AFTER parent thread has returned (in eventlet it executes in the same context)

    from nameko.concurrency.gevent import *
