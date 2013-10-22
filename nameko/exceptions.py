import traceback


class MethodNotFound(AttributeError):
    pass


class WaiterTimeout(Exception):
    pass


class RemoteError(Exception):
    def __init__(self, exc_type=None, value=None, traceback=None):
        self.exc_type = exc_type
        self.value = value
        self.traceback = traceback
        message = '{} {}\n{}'.format(exc_type, value, traceback)
        super(RemoteError, self).__init__(message)


class RemoteErrorWrapper(object):
    def __init__(self, exc):
        self.exc = exc
        self.traceback = traceback.format_exc()

    def serialize(self):
        return {
            'exc_type': self.exc.__class__.__name__,
            'value': self.exc.message,
            'traceback': self.traceback
        }

    @classmethod
    def deserialize(cls, data):
        return RemoteError(**data)
