from nameko import exceptions


registry = {}


def get_exception_info(exc):
    if not isinstance(exc, basestring):
        exc = exceptions.get_module_path(exc.__class__)
    return registry.get(exc)


def expose_exception(exc):
    data = exceptions.serialize(exc)
    info = get_exception_info(exc)
    if info is None:
        return 400, data
    return info['status'], data


def web_exception(status):
    def decorator(cls):
        registry[exceptions.get_module_path(cls)] = {
            'status': status
        }
        cls
    return decorator


web_exception(404)(exceptions.MethodNotFound)
