from __future__ import unicode_literals

import collections
import inspect

import six


class ExtensionNotFound(AttributeError):
    pass


class RpcTimeout(Exception):
    pass


class RpcConnectionError(Exception):
    """Raised (in the caller) if the connection to the broker is lost while
    waiting for an rpc reply"""


class ContainerBeingKilled(Exception):
    """Raised by :meth:`Container.spawn_worker` if it has started a ``kill``
    sequence.

    Entrypoints should catch this and react as if they hadn't been available
    in the first place, e.g. an rpc consumer should probably requeue
    the message.

    We need this because eventlet may yield during the execution of
    :meth:`Container.kill`, giving entrypoints a chance to fire before
    they themselves have been killed.
    """


registry = {}


def get_module_path(exc_type):
    """ Return the dotted module path of `exc_type`, including the class name.

    e.g.::

        >>> get_module_path(MethodNotFound)
        >>> "nameko.exceptions.MethodNotFound"

    """
    module = inspect.getmodule(exc_type)
    return "{}.{}".format(module.__name__, exc_type.__name__)


class RemoteError(Exception):
    """ Exception to raise at the caller if an exception occurred in the
    remote worker.
    """
    def __init__(self, exc_type=None, value=""):
        self.exc_type = exc_type
        self.value = value
        message = '{} {}'.format(exc_type, value)
        super(RemoteError, self).__init__(message)


def safe_for_serialization(value):
    """ Transform a value in preparation for serializing as json

    no-op for strings, mappings and iterables have their entries made safe,
    and all other values are stringified, with a fallback value if that fails
    """

    if isinstance(value, six.string_types):
        return value
    if isinstance(value, dict):
        return {
            safe_for_serialization(key): safe_for_serialization(val)
            for key, val in six.iteritems(value)
        }
    if isinstance(value, collections.Iterable):
        return list(map(safe_for_serialization, value))

    try:
        return six.text_type(value)
    except Exception:
        return '[__unicode__ failed]'


def serialize(exc):
    """ Serialize `self.exc` into a data dictionary representing it.
    """

    return {
        'exc_type': type(exc).__name__,
        'exc_path': get_module_path(type(exc)),
        'exc_args': list(map(safe_for_serialization, exc.args)),
        'value': safe_for_serialization(exc),
    }


def deserialize(data):
    """ Deserialize `data` to an exception instance.

    If the `exc_path` value matches an exception registered as
    ``deserializable``, return an instance of that exception type.
    Otherwise, return a `RemoteError` instance describing the exception
    that occurred.
    """
    key = data.get('exc_path')
    if key in registry:
        exc_args = data.get('exc_args', ())
        return registry[key](*exc_args)

    exc_type = data.get('exc_type')
    value = data.get('value')
    return RemoteError(exc_type=exc_type, value=value)


def deserialize_to_instance(exc_type):
    """ Decorator that registers `exc_type` as deserializable back into an
    instance, rather than a :class:`RemoteError`. See :func:`deserialize`.
    """
    key = get_module_path(exc_type)
    registry[key] = exc_type
    return exc_type


class BadRequest(Exception):
    pass


@deserialize_to_instance
class MalformedRequest(BadRequest):
    pass


@deserialize_to_instance
class MethodNotFound(BadRequest):
    pass


@deserialize_to_instance
class IncorrectSignature(BadRequest):
    pass


class UnknownService(Exception):
    def __init__(self, service_name):
        self._service_name = service_name
        super(UnknownService, self).__init__(service_name)

    def __str__(self):
        return "Unknown service `{}`".format(self._service_name)


class UnserializableValueError(Exception):
    def __init__(self, value):
        try:
            self.repr_value = repr(value)
        except Exception:
            self.repr_value = '[__repr__ failed]'
        super(UnserializableValueError, self).__init__()

    def __str__(self):
        return "Unserializable value: `{}`".format(self.repr_value)


class ConfigurationError(Exception):
    pass


class CommandError(Exception):
    """Raise from subcommands to report error back to the user"""


class ConnectionNotFound(BadRequest):
    """Unknown websocket connection id"""
