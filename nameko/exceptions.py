class MethodNotFound(AttributeError):
    pass


class WaiterTimeout(Exception):
    pass


class RemoteError(Exception):
    def __init__(self, exc_type=None, value=None):
        self.exc_type = exc_type
        self.value = value
        message = '{} {}'.format(exc_type, value)
        super(RemoteError, self).__init__(message)


class RemoteErrorWrapper(object):
    def __init__(self, exc):
        self.exc = exc

    def serialize(self):
        return {
            'exc_type': self.exc.__class__.__name__,
            'value': self.exc.message,
        }

    @classmethod
    def deserialize(cls, data):
        return RemoteError(**data)


class DependencyNotFound(AttributeError):
    pass
