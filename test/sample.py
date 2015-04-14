from nameko.rpc import rpc


class Service(object):
    name = "service"

    @rpc
    def ping(self):
        pass
