from nameko.rpc import rpc


def service_class(cls):
    return cls

@service_class
class ExampleService(object):
    @rpc
    def method(self):
        pass
