from nameko.rpc import rpc, RpcProxy

class Service(object):
    name = "service"

    # we depend on the RPC interface of "another_service"
    other_rpc = RpcProxy("another_service")

    @rpc  # `method` is exposed over RPC
    def method(self):
        # application logic goes here
        pass
