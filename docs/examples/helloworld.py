from nameko.rpc import rpc

class HelloWorld(object):
    name = "hello_world"

    @rpc
    def hello(self, name):
        return "Hello, {}!".format(name)
