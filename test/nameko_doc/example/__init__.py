from nameko.rpc import rpc


class ExampleService(object):
    @rpc
    def method(self):
        pass

    def red_herring(self):
        pass
