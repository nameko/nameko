from __future__ import absolute_import
from logging import getLogger

from nameko.dependencies import (
    entrypoint, EntrypointProvider, DependencyFactory)

_log = getLogger(__name__)


class OnceProvider(EntrypointProvider):
    def __init__(self, *args, **kwargs):
        pass

    def start(self):
        _log.debug('starting %s', self)
        args = tuple()
        kwargs = {}
        self.container.spawn_worker(self, args, kwargs)


@entrypoint
def once(interval=None, config_key=None):
    """
    Decorates a method to be executed as a one-off in a service.

    This is useful in testing for triggering work to start, where you don't
    want the open-ended nature of the `timer` entry-point.

    Example:

    class Foobar(object):

        @once()
        def handle_timer(self):
            self.shrub(body)
    """
    return DependencyFactory(OnceProvider, interval, config_key)
