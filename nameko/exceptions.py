import inspect


class DependencyNotFound(AttributeError):
    pass


class WaiterTimeout(Exception):
    pass


registry = {}


def get_module_path(exc_class):
    """ Return the dotted module path of `exc_class`
    """
    module = inspect.getmodule(exc_class)
    return "{}.{}".format(module.__name__, exc_class.__name__)


def deserializable(exc_class):
    """ Decorator that registers `exc_class` as deserializable back into a
    class instance, rather than a :class:`RemoteError`.
    See :meth:`RemoteErrorWrapper.deserialize`.
    """
    key = get_module_path(exc_class)
    registry[key] = exc_class
    return exc_class


@deserializable
class MethodNotFound(Exception):
    pass


@deserializable
class IncorrectSignature(Exception):
    pass


class RemoteError(Exception):
    """ Exception to raise at the caller if an exception occured in the
    remote worker.
    """
    def __init__(self, exc_type=None, value="", args=()):
        self.exc_type = exc_type
        self.value = value
        self.args = args
        message = '{} {}'.format(exc_type, value)
        super(RemoteError, self).__init__(message)


class RemoteErrorWrapper(object):
    """ Encapsulate an exception and provide serialization and
    deserialization for it.
    """
    def __init__(self, exc):
        self.exc = exc

    def serialize(self):
        """ Serialize `self.exc` into a data dictionary representing it.
        """
        return {
            'exc_type': type(self.exc).__name__,
            'exc_path': get_module_path(type(self.exc)),
            'value': self.exc.message,
            'args': self.exc.args,
        }

    @classmethod
    def deserialize(cls, data):
        """ Deserialize `data` to an exception instance.

        If the `exc_path` value matches an exception registered as
        ``deserializable``, return an instance of that exception type.
        Otherwise, return a `RemoteError` instance describing the exception
        that occured.
        """
        key = data['exc_path']
        if key in registry:
            return registry[key](*data['args'])

        args = data.copy()
        del args['exc_path']
        return RemoteError(**args)


class UnknownService(Exception):
    def __init__(self, service_name):
        self._service_name = service_name
        super(UnknownService, self).__init__(service_name)

    def __str__(self):
        return "Unknown service `{}`".format(self._service_name)
