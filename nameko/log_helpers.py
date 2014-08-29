from __future__ import absolute_import

from contextlib import contextmanager
import logging
import time


def make_timing_logger(logger, precision=3, level=logging.DEBUG):
    """ Return a timing logger.

    Usage::

        >>> logger = logging.getLogger('foobar')
        >>> log_time = make_timing_logger(
        ...     logger, level=logging.INFO, precision=2)
        >>>
        >>> with log_time("hello %s", "world"):
        ...     time.sleep(1)
        INFO:foobar:hello world in 1.00s
    """
    @contextmanager
    def log_time(msg, *args):
        """ Log `msg` and `*args` with (naive wallclock) timing information
        when the context block exits.
        """
        start_time = time.time()

        try:
            yield
        finally:
            message = "{} in %0.{}fs".format(msg, precision)
            duration = time.time() - start_time
            args = args + (duration,)
            logger.log(level, message, *args)

    return log_time
