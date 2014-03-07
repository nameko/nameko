"""
This is example
"""
from .other import Ignore


def service_class(cls):
    return cls


def service_method(fn):
    return fn


class RedHerringService(object):
    pass


@service_class
class ExampleService(object):
    @service_method
    def method(self):
        pass
