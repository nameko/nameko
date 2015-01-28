from nameko.rpc import rpc


class Service(object):
    @rpc
    def ping(self):
        pass
