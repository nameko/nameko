from nameko import exceptions


class ConnectionNotFound(LookupError):
    pass


class BadPayload(Exception):
    pass


operational_errors = frozenset([
    exceptions.IncorrectSignature,
    exceptions.MalformedRequest,
    exceptions.MethodNotFound,
    ConnectionNotFound,
    BadPayload,
])


def expose_exception(exc):
    if exc.__class__ in operational_errors:
        is_operational = True
    else:
        is_operational = False
    return is_operational, {
        'type': '%s.%s' % (exc.__class__.__module__, exc.__class__.__name__),
        'message': str(exc),
    }
