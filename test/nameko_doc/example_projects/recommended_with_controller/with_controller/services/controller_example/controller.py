import utils


def service_class(cls):
    return cls


class RedHerringService(object):
    pass


@service_class
class ExampleService(object):
    @utils.service_method
    def method(self):
        pass
