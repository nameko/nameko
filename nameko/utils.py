import inspect
import re
import sys
from pydoc import locate

import eventlet
import six
from eventlet.queue import LightQueue

REDACTED = "********"


def get_redacted_args(entrypoint, *args, **kwargs):
    """ Utility function for use with entrypoints that are marked with
    ``sensitive_variables`` -- e.g. :class:`nameko.rpc.Rpc` and
    :class:`nameko.events.EventHandler`.

    :Parameters:
        entrypoint : :class:`~nameko.extensions.Entrypoint`
            The entrypoint that fired.
        args : tuple
            Positional arguments for the method call.
        kwargs : dict
            Keyword arguments for the method call.

    The entrypoint should have a ``sensitive_variables`` attribute, the value
    of which is a string or tuple of strings specifying the arguments or
    partial arguments that should be redacted. To partially redact an argument,
    the following syntax is used::

        <argument-name>.<dict-key>[<list-index>]

    :Returns:
        A dictionary as returned by :func:`inspect.getcallargs`, but with
        sensitive arguments or partial arguments redacted.

    .. note::

        This function does not raise if one of the ``sensitive_variables``
        doesn't match or partially match the calling ``args`` and ``kwargs``.
        This allows "fuzzier" pattern matching (e.g. redact a field if it is
        present, and otherwise do nothing).

        To avoid exposing sensitive variables through a typo, it is recommend
        to test the configuration of each entrypoint with
        ``sensitive_variables`` individually. For example:

        .. code-block:: python

            class Service(object):
                @rpc(sensitive_variables="foo.bar")
                def method(self, foo):
                    pass

            container = ServiceContainer(Service, {})
            entrypoint = get_extension(container, Rpc, method_name="method")

            # no redaction
            foo = "arg"
            expected_foo = {'foo': "arg"}
            assert get_redacted_args(entrypoint, foo) == expected

            # 'bar' key redacted
            foo = {'bar': "secret value", 'baz': "normal value"}
            expected = {'foo': {'bar': "********", 'baz': "normal value"}}
            assert get_redacted_args(entrypoint, foo) == expected

    .. seealso::

        The tests for this utility demonstrate its full usage:
        :class:`test.test_utils.TestGetRedactedArgs`

    """
    sensitive_variables = entrypoint.sensitive_variables
    if isinstance(sensitive_variables, six.string_types):
        sensitive_variables = (sensitive_variables,)

    method = getattr(entrypoint.container.service_cls, entrypoint.method_name)
    callargs = inspect.getcallargs(method, None, *args, **kwargs)
    del callargs['self']

    def redact(data, keys):
        key = keys[0]
        if len(keys) == 1:
            try:
                data[key] = REDACTED
            except (KeyError, IndexError, TypeError):
                pass
        else:
            if key in data:
                redact(data[key], keys[1:])

    for variable in sensitive_variables:
        keys = []
        for dict_key, list_index in re.findall("(\w+)|\[(\d+)\]", variable):
            if dict_key:
                keys.append(dict_key)
            elif list_index:
                keys.append(int(list_index))

        if keys[0] in callargs:
            redact(callargs, keys)

    return callargs


def fail_fast_imap(pool, call, items):
    """ Run a function against each item in a given list, yielding each
    function result in turn, where the function call is handled in a
    :class:`~eventlet.greenthread.GreenThread` spawned by the provided pool.

    If any function raises an exception, all other ongoing threads are killed,
    and the exception is raised to the caller.

    This function is similar to :meth:`~eventlet.greenpool.GreenPool.imap`.

    :param pool: Pool to spawn function threads from
    :type pool: eventlet.greenpool.GreenPool
    :param call: Function call to make, expecting to receive an item from the
        given list
    """
    result_queue = LightQueue(maxsize=len(items))
    spawned_threads = set()

    def handle_result(finished_thread):
        try:
            thread_result = finished_thread.wait()
            spawned_threads.remove(finished_thread)
            result_queue.put((thread_result, None))
        except Exception:
            spawned_threads.remove(finished_thread)
            result_queue.put((None, sys.exc_info()))

    for item in items:
        gt = pool.spawn(call, item)
        spawned_threads.add(gt)
        gt.link(handle_result)

    while spawned_threads:
        result, exc_info = result_queue.get()
        if exc_info is not None:
            # Kill all other ongoing threads
            for ongoing_thread in spawned_threads:
                ongoing_thread.kill()
            # simply raising here (even raising a full exc_info) isn't
            # sufficient to preserve the original stack trace.
            # greenlet.throw() achieves this.
            eventlet.getcurrent().throw(*exc_info)
        yield result


class SpawningProxy(object):
    def __init__(self, items, abort_on_error=False):
        """ Wraps an iterable set of items such that a call on the returned
        SpawningProxy instance will spawn a call in a
        :class:`~eventlet.greenthread.GreenThread` for each item.

        Returns when every spawned thread has completed.

        :param items: Iterable item set to process
        :param abort_on_error: If True, any exceptions raised on an individual
            item call will cause all peer item call threads to be killed, and
            for the exception to be propagated to the caller immediately.
        """
        self._items = items
        self.abort_on_error = abort_on_error

    def __getattr__(self, name):

        def spawning_method(*args, **kwargs):
            items = self._items
            if items:
                pool = eventlet.GreenPool(len(items))

                def call(item):
                    return getattr(item, name)(*args, **kwargs)

                if self.abort_on_error:
                    return list(fail_fast_imap(pool, call, self._items))
                else:
                    return list(pool.imap(call, self._items))
        return spawning_method


class SpawningSet(set):
    """ A set with an ``.all`` property that will spawn a method call on each
    item in the set into its own (parallel) greenthread.
    """
    @property
    def all(self):
        return SpawningProxy(self)


def import_from_path(path):
    """ Import and return the object at `path` if it exists.

    Raises an :exc:`ImportError` if the object is not found.
    """
    if path is None:
        return

    obj = locate(path)
    if obj is None:
        raise ImportError(
            "`{}` could not be imported".format(path)
        )

    return obj
