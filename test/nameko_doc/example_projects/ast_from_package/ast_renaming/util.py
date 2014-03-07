def service_class(x):
    def dec(cls):
        return cls
    return dec


def service_method(fn):
    return fn


def misdirection(fn):
    return fn
