import inspect
import re
from copy import deepcopy
from pydoc import locate
from six.moves.urllib.parse import urlparse

import six

REDACTED = "********"


def get_redacted_args(entrypoint, *args, **kwargs):
    """ Utility function for use with entrypoints that are marked with
    ``sensitive_arguments`` -- e.g. :class:`nameko.rpc.Rpc` and
    :class:`nameko.events.EventHandler`.

    :Parameters:
        entrypoint : :class:`~nameko.extensions.Entrypoint`
            The entrypoint that fired.
        args : tuple
            Positional arguments for the method call.
        kwargs : dict
            Keyword arguments for the method call.

    The entrypoint should have a ``sensitive_arguments`` attribute, the value
    of which is a string or tuple of strings specifying the arguments or
    partial arguments that should be redacted. To partially redact an argument,
    the following syntax is used::

        <argument-name>.<dict-key>[<list-index>]

    :Returns:
        A dictionary as returned by :func:`inspect.getcallargs`, but with
        sensitive arguments or partial arguments redacted.

    .. note::

        This function does not raise if one of the ``sensitive_arguments``
        doesn't match or partially match the calling ``args`` and ``kwargs``.
        This allows "fuzzier" pattern matching (e.g. redact a field if it is
        present, and otherwise do nothing).

        To avoid exposing sensitive arguments through a typo, it is recommend
        to test the configuration of each entrypoint with
        ``sensitive_arguments`` individually. For example:

        .. code-block:: python

            class Service(object):
                @rpc(sensitive_arguments="foo.bar")
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
    sensitive_arguments = entrypoint.sensitive_arguments
    if isinstance(sensitive_arguments, six.string_types):
        sensitive_arguments = (sensitive_arguments,)

    method = getattr(entrypoint.container.service_cls, entrypoint.method_name)
    callargs = inspect.getcallargs(method, None, *args, **kwargs)
    del callargs['self']

    # make a deepcopy before redacting so that "partial" redacations aren't
    # applied to a referenced object
    callargs = deepcopy(callargs)

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

    for variable in sensitive_arguments:
        keys = []
        for dict_key, list_index in re.findall(r"(\w+)|\[(\d+)\]", variable):
            if dict_key:
                keys.append(dict_key)
            elif list_index:
                keys.append(int(list_index))

        if keys[0] in callargs:
            redact(callargs, keys)

    return callargs


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


def sanitize_url(url):
    """Redact password in urls."""
    parts = urlparse(url)
    if parts.password is None:
        return url
    host_info = parts.netloc.rsplit('@', 1)[-1]
    parts = parts._replace(netloc='{}:{}@{}'.format(
        parts.username, REDACTED, host_info))
    return parts.geturl()
