class DependencyNotFound(AttributeError):
    pass


class WaiterTimeout(Exception):
    pass


registry = {}


def deserializable(exc_class):
    registry[exc_class.__name__] = exc_class
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
            'exc_type': self.exc.__class__.__name__,
            'value': self.exc.message,
            'args': self.exc.args,
        }

    @classmethod
    def deserialize(cls, data):
        """ Deserialize `data` to an exception instance.

        If the `exc_type` key matches an exception registered as
        ``deserializable``, return an instance of that exception type.
        Otherwise, return a `RemoteError` instance describing the exception
        that occured.
        """
        exc_type_name = data['exc_type']
        if exc_type_name in registry:
            return registry[exc_type_name](*data['args'])

        return RemoteError(**data)


class UnknownService(Exception):
    def __init__(self, service_name):
        self._service_name = service_name
        super(UnknownService, self).__init__(service_name)

    def __str__(self):
        return "Unknown service `{}`".format(self._service_name)
