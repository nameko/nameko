class MethodNotFound(AttributeError):
    pass


class WaiterTimeout(Exception):
    pass


class RemoteError(Exception):
    def __init__(self, exc_type=None, value=None, traceback=None):
        self.exc_type = exc_type
        self.value = value
        self.traceback = traceback
        message = 'Remote error: {} {}\n{}'.format(exc_type, value, traceback)
        super(RemoteError, self).__init__(message)
