import functools
import eventlet


class SpawningProxy(object):
    """ Wraps an iterable set of items such that a call on the SpawningProxy
    will spawn a call in a greenthread for each item.

    Returns when every spawned thread has completed.
    """
    def __init__(self, items):
        self._items = items

    def __getattr__(self, name):

        def spawning_method(*args, **kwargs):
            items = self._items
            if items:
                pool = eventlet.GreenPool(len(items))

                def call(item):
                    return getattr(item, name)(*args, **kwargs)
                list(pool.imap(call, self._items))

        return spawning_method


class SpawningSet(set):
    """ A set with an ``.all`` property that will spawn a method call on each
    item in the set into its own (parallel) greenthread.
    """
    @property
    def all(self):
        return SpawningProxy(self)


def try_wraps(func):
    """Marks a function as wrapping another one using `functools.wraps`, but
    fails unobtrusively when this isn't possible"""
    do_wrap = functools.wraps(func)

    def try_to_wrap(inner):
        try:
            return do_wrap(inner)
        except AttributeError:
            # Some objects don't have all the attributes needed in order to
            # be wrapped. Fail gracefully and don't wrap.
            return inner

    return try_to_wrap
