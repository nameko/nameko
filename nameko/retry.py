import functools
import itertools
from time import sleep

import wrapt


class RetryDelay(object):
    def __init__(self, delay, backoff, max_delay):
        self.delay = delay
        self.backoff = backoff
        self.max_delay = max_delay

    def next(self):
        if self.backoff:
            self.delay *= self.backoff

        if self.max_delay:
            return min(self.delay, self.max_delay)

        return self.delay


def retry(
    wrapped=None, allowed_exceptions=None,
    retries=3, delay=1, backoff=1, max_delay=None
):
    if wrapped is None:
        return functools.partial(
            retry, allowed_exceptions=allowed_exceptions,
            retries=retries, delay=delay, backoff=backoff, max_delay=max_delay
        )

    allowed_exceptions = allowed_exceptions or Exception
    if retries is None:
        retries = float('inf')

    retry_delay = RetryDelay(delay, backoff, max_delay)

    @wrapt.decorator
    def wrapper(wrapped, instance, args, kwargs):

        counter = itertools.count()

        while True:
            try:
                return wrapped(*args, **kwargs)
            except allowed_exceptions:
                if counter.next() == retries:
                    raise
                sleep(retry_delay.next())

    return wrapper(wrapped)
