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
    wrapped=None, for_exceptions=Exception, max_attempts=3,
    delay=1, backoff=1, max_delay=None
):
    if wrapped is None:
        return functools.partial(
            retry, for_exceptions=for_exceptions, max_attempts=max_attempts,
            delay=delay, backoff=backoff, max_delay=max_delay
        )

    if max_attempts is None:
        max_attempts = float('inf')

    retry_delay = RetryDelay(delay, backoff, max_delay)

    @wrapt.decorator
    def wrapper(wrapped, instance, args, kwargs):

        counter = itertools.count()

        while True:
            try:
                return wrapped(*args, **kwargs)
            except for_exceptions:
                if next(counter) == max_attempts:
                    raise
                sleep(retry_delay.next())

    return wrapper(wrapped)  # pylint: disable=E1120
