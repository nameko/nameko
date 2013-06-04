from contextlib import contextmanager
import time


@contextmanager
def log_time(log_method, msg, *args):
    """ Logs the time it took from entering to exiting the context manager.

        The `msg` will be passed to the `log_method` along the the `args`
        appended with the timed duration.

    example:
        with log_time(log.debug, "It took %0.3f sec to call %s", "shrub()"):
            shrub()
    """
    start_time = time.time()

    try:
        yield
    finally:
        duration = time.time() - start_time
        args = args + (duration,)
        log_method(msg, *args)
