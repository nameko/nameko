import eventlet


class SpawningProxy(object):
    """ Wraps an iterable set of items such that a call on the SpawningProxy
    will spawn a call in a greenthread for each item.

    Returns when every spawned thread has completed.
    """
    def __init__(self, items):
        self._items = items

    def __getattr__(self, name):

        def fn(*args, **kwargs):
            items = self._items
            pool = eventlet.GreenPool(len(items))

            call = lambda item: getattr(item, name)(*args, **kwargs)
            list(pool.imap(call, self._items))

        return fn


class SpawningSet(set):
    """ A set with an ``.all`` property that will spawn a method call on each
    item in the set into its own (parallel) greenthread.
    """
    @property
    def all(self):
        return SpawningProxy(self)
