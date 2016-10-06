from nameko.rpc import rpc


class ExampleService(object):
    name = 'nameko_example'

    @rpc
    def method(self):
        pass  # pragma: no cover

    def red_herring(self):
        pass  # pragma: no cover
