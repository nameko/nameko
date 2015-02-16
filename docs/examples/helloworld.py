from nameko.rpc import rpc


class HelloWorld(object):

    @rpc
    def hello(self, name):
        return "Hello, {}!".format(name)
